import requests
import cv2
import numpy as np
from ultralytics import YOLO
import time
import logging
import datetime
from kafka import KafkaProducer
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import orjson
from requests.adapters import HTTPAdapter, Retry
import os
import pytz

CAMERA_JSON_FILE = "cameras.json" 
BASE_URL = "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id="
BATCH_SIZE = 20          # batch cực nhỏ cho 2GB RAM
BATCH_DELAY = 30         # delay 30s giữa batch
MAX_WORKERS = 3          # 1 thread để tiết kiệm RAM + CPU
YOLO_CONF = 0.1
TARGET_CLASSES = ['car', 'motorcycle', 'bus', 'truck']
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.5
RESIZE_WIDTH = 320       # resize nhỏ để giảm RAM
RESIZE_HEIGHT = 180

# ===============================
# Logging
# ===============================
logging.basicConfig(filename='producer.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# ===============================
# Kafka setup
# ===============================

# Read bootstrap servers from env (comma-separated), default to in-cluster Service
# For local testing with port-forward, set KAFKA_BOOTSTRAP_SERVERS=localhost:9094
_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka.hugedata.svc.cluster.local:9092')
KAFKA_BOOTSTRAP_SERVERS = [s.strip() for s in _bootstrap.split(',') if s.strip()]

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        linger_ms=100
    )


producer = create_kafka_producer()

# ===============================
# YOLO setup (Ultralytics v8 API, force CPU at inference time)
# Using the official ultralytics:latest-python image may already provide the
# correct device and runtime; we explicitly request CPU when calling predict.
# ===============================
model = YOLO('best.pt')

# ===============================
# Requests session with retry
# ===============================
def create_session():
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = create_session()

def parse_location(loc_str):
    if not loc_str:
        return None, None

    try:
        loc_json = orjson.loads(loc_str)
        shape = loc_json["Rows"][0][1]
        match = re.search(r"POINT\(([\d.]+)\s+([\d.]+)\)", shape)
        if match:
            lon = float(match.group(1))
            lat = float(match.group(2))
            return lat, lon
    except Exception:
        pass

    return None, None

# ===============================
# Load cameras once, cache in memory
# ===============================
def load_cameras(path):
    with open(path, 'rb') as f:
        data = orjson.loads(f.read())
    if isinstance(data, dict):
        data = [data]

    cameras = []
    for cam in data:
        cam_id = cam.get("CamId")
        display_name = cam.get("DisplayName")
        loc_str = cam.get("Location")
        lat, lon = parse_location(loc_str)

        cameras.append({
            "cam_id": cam_id,
            "display_name": display_name,
            "latitude": lat,
            "longitude": lon
        })
    return cameras

CAMERAS_CACHE = load_cameras(CAMERA_JSON_FILE)
TOTAL_CAMERAS = len(CAMERAS_CACHE)
print(f"Loaded {TOTAL_CAMERAS} cameras.")

# ===============================
# Fetch + YOLO processing
# ===============================
def process_camera(cam):
    cam_id = cam["cam_id"]
    url = f"{BASE_URL}{cam_id}&t={int(time.time() * 1000)}"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = session.get(url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()
        if 'image/jpeg' not in response.headers.get('Content-Type', ''):
            raise ValueError("Invalid content type")

        img_array = np.asarray(bytearray(response.raw.read()), dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        img = cv2.resize(img, (RESIZE_WIDTH, RESIZE_HEIGHT))

        # Run prediction on CPU to avoid CUDA requirements in the container
        results = model.predict(source=img, conf=YOLO_CONF, device='cpu', verbose=False)[0]

        # Extract class indices from results.boxes.cls in a safe way (handles
        # torch tensors or numpy arrays depending on runtime). If there are no
        # boxes, results.boxes may be empty.
        classes = []
        if getattr(results, 'boxes', None) is not None and len(results.boxes) > 0:
            try:
                cls_data = results.boxes.cls
                # If it's a torch tensor, move to cpu and convert to numpy
                if hasattr(cls_data, 'cpu'):
                    cls_array = cls_data.cpu().numpy()
                else:
                    cls_array = cls_data
                classes = [int(x) for x in cls_array]
            except Exception:
                # Fallback: iterate and coerce to int
                classes = [int(x) for x in results.boxes.cls]

        vehicles = [model.names[c] for c in classes if model.names[c] in TARGET_CLASSES]
        vehicle_counts = dict(Counter(vehicles))

        car_count = vehicle_counts.get("car", 0)
        truck_count = vehicle_counts.get("truck", 0)
        bus_count = vehicle_counts.get("bus", 0)
        motorcycle_count = vehicle_counts.get("motorcycle", 0)

        vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time_vn = datetime.datetime.now(vietnam_tz)
        # data format
        # time, camera_id, latitude, longitude, camera, car_count, truck_count, bus_count, motorcycle_count
        data = {
            "time": current_time_vn.strftime("%Y-%m-%d %H:%M:%S"),
            "camera_id": cam_id,
            "latitude": cam["latitude"],
            "longitude": cam["longitude"],
            "camera": cam["display_name"],
            "car_count": car_count,
            "bus_count": bus_count,
            "truck_count": truck_count,
            "motorcycle_count": motorcycle_count,
            "total_count": car_count + bus_count + truck_count + motorcycle_count
        }

        producer.send('traffic', value=data)
        return f"[OK] {current_time_vn.strftime('%Y-%m-%d %H:%M:%S')} {cam_id}: {vehicle_counts}"

    except Exception as e:
        return f"[ERROR] {cam_id}: {e}"

# ===============================
# Main loop với batch cực nhỏ + delay
# ===============================
def main():
    while True:
        start_time = time.time()
        print(f"\n=== Fetch round at {datetime.datetime.now()} ===")

        for i in range(0, TOTAL_CAMERAS, BATCH_SIZE):
            batch = CAMERAS_CACHE[i:i+BATCH_SIZE]
            print(f"\nProcessing batch {i//BATCH_SIZE + 1}: {len(batch)} cameras")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_camera, cam) for cam in batch]
                for f in as_completed(futures):
                    result = f.result()
                    print(result)
                    logging.info(result)

            producer.flush()
            print(f"Batch {i//BATCH_SIZE + 1} done, waiting {BATCH_DELAY}s before next batch...")
            time.sleep(BATCH_DELAY)

        elapsed = time.time() - start_time
        print(f"Round done in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
