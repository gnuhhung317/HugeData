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

CAMERA_JSON_FILE = "kafka/cameras.json"  # Dùng khi chạy từ root folder
# CAMERA_JSON_FILE = "cameras.json"  # Uncomment nếu chạy từ trong kafka folder
BASE_URL = "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id="
BATCH_SIZE = 1          # batch cực nhỏ cho 2GB RAM
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
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks='all',
    linger_ms=100
)

# ===============================
# YOLO setup (Ultralytics v8 API, force CPU at inference time)
# Using the official ultralytics:latest-python image may already provide the
# correct device and runtime; we explicitly request CPU when calling predict.
# ===============================
model = YOLO('yolov8n.pt')

# ===============================
# Requests session with retry
# ===============================
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
        lat = lon = None
        if loc_str:
            try:
                loc_json = orjson.loads(loc_str)
                shape = loc_json["Rows"][0][1]
                match = re.search(r"POINT\(([\d.]+)\s+([\d.]+)\)", shape)
                if match:
                    lon, lat = float(match.group(1)), float(match.group(2))
            except Exception:
                pass
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

        data = {
            "camera": cam["display_name"],
            "camera_id": cam_id,
            "latitude": cam["latitude"],
            "longitude": cam["longitude"],
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "counts": vehicle_counts
        }

        producer.send('traffic', value=data)
        return f"[OK] {cam_id}: {vehicle_counts}"

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
