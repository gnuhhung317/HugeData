import asyncio, aiohttp, time, random, hashlib, json, os, logging
from aiolimiter import AsyncLimiter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Đọc camera IDs từ file JSON
try:
    with open('crawl/camera_ids.json', 'r', encoding='utf-8') as f:
        CAMERA_IDS = json.load(f)
except FileNotFoundError:
    logging.error("File 'crawl/camera_ids.json' not found.")
    exit(1)
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON from 'crawl/camera_ids.json': {e}")
    exit(1)

# Đảm bảo không trùng lặp
CAMERA_IDS = list(set(CAMERA_IDS))
logging.info(f"Tổng số camera IDs: {len(CAMERA_IDS)}")

BASE_URL = "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"

HEADERS = {
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
}

COOKIE = "ASP.NET_SessionId=...; .VDMS=...; CurrentLanguage=vi; ..."  # Cập nhật cookie định kỳ

RPS_LIMIT = 140
PERIOD_SEC = 1500
limiter = AsyncLimiter(RPS_LIMIT, time_period=1)

def epoch_ms():
    return int(time.time() * 1000)

def phash_stub(data: bytes) -> str:
    # Placeholder: dùng imagehash ở prod
    return hashlib.sha1(data).hexdigest()[:16]

async def fetch_one(session, camera_id):
    params = {"id": camera_id, "bg": "black", "w": "300", "h": "230", "t": str(epoch_ms())}
    async with limiter:
        for attempt in range(3):
            try:
                async with session.get(BASE_URL, params=params, timeout=2) as resp:
                    if resp.status != 200:
                        # logging.warning(f"Camera {camera_id}: HTTP {resp.status}, attempt {attempt+1}")
                        await asyncio.sleep(0.2 * (attempt + 1))
                        continue
                    data = await resp.read()
                    if len(data) < 500:  # Ảnh lỗi/trống
                        logging.warning(f"Camera {camera_id}: Image too small ({len(data)} bytes)")
                        return None
                    return data
            except Exception as e:
                # logging.error(f"Camera {camera_id}: Fetch error on attempt {attempt+1}: {e}")
                await asyncio.sleep(0.3 * (attempt + 1))
    return None

async def worker(name, shard, barrier):
    timeout = aiohttp.ClientTimeout(total=4)
    cookie_hdr = {"Cookie": COOKIE}
    async with aiohttp.ClientSession(headers={**HEADERS, **cookie_hdr}, timeout=timeout) as session:
        # Tạo thư mục images nếu chưa có
        try:
            os.makedirs('images', exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create images directory: {e}")
            return
        while True:
            await barrier.wait()  # Đồng bộ bắt đầu quét
            start_time = time.time()
            logging.info(f"[{name}] Bắt đầu quét {len(shard)} camera lúc {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
            t0 = time.time()
            tasks = [fetch_one(session, cid) for cid in shard]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            now = int(time.time())
            for cid, img in zip(shard, results):
                if not img:
                    continue
                h = phash_stub(img)
                # Lưu ảnh vào file
                filename = f"images/{cid}_{now}.jpg"
                try:
                    with open(filename, 'wb') as f:
                        f.write(img)
                    logging.info(f"Lưu ảnh: {filename}, phash: {h}")
                except Exception as e:
                    logging.error(f"Failed to save image {filename}: {e}")
                # Push Kafka metadata: {camera_id: cid, ts: now, key: filename, phash: h, size: len(img)}
            # Ngủ phần còn lại của 5 giây
            dt = time.time() - t0
            end_time = time.time()
            scan_duration = end_time - start_time
            logging.info(f"[{name}] Kết thúc quét lúc {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}, thời gian quét: {scan_duration:.2f}s")
            await barrier.wait()  # Đồng bộ kết thúc quét
            if name == "W0":  # Chỉ worker đầu tiên in thời gian toàn bộ
                total_scan_time = time.time() - start_time
                logging.info(f"Thời gian quét toàn bộ {len(CAMERA_IDS)} camera: {total_scan_time:.2f}s")
            await asyncio.sleep(max(0, PERIOD_SEC - dt + random.uniform(0, 0.15)))

async def main():
    # Chia camera IDs cho N worker
    N = 10
    shards = [CAMERA_IDS[i::N] for i in range(N)]
    barrier = asyncio.Barrier(N)
    await asyncio.gather(*(worker(f"W{i}", shards[i], barrier) for i in range(N)))

if __name__ == "__main__":
    asyncio.run(main())