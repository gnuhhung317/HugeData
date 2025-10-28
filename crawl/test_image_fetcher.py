import asyncio, aiohttp, time, random, hashlib
from aiolimiter import AsyncLimiter

CAMERAS = [
    # (camera_id, url_base)
    ("6792f16e8c5ed4001b27f482", "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"),
    # ... 700 camera ở đây
]

HEADERS = {
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
}

COOKIE = "ASP.NET_SessionId=...; .VDMS=...; CurrentLanguage=vi; ..."  # làm mới định kỳ

RPS_LIMIT = 140
PERIOD_SEC = 5
limiter = AsyncLimiter(RPS_LIMIT, time_period=1)

def epoch_ms():
    return int(time.time() * 1000)

def phash_stub(data: bytes) -> str:
    # placeholder: dùng imagehash ở prod
    return hashlib.sha1(data).hexdigest()[:16]

async def fetch_one(session, camera_id, base_url):
    # query: t=epoch_ms, id=camera_id, w/h/bg như mẫu
    params = {"id": camera_id, "bg": "black", "w": "300", "h": "230", "t": str(epoch_ms())}
    async with limiter:
        for attempt in range(3):
            try:
                async with session.get(base_url, params=params, timeout=2) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(0.2 * (attempt + 1))
                        continue
                    data = await resp.read()
                    if len(data) < 500:  # ảnh lỗi/trống
                        return None
                    return data
            except Exception:
                await asyncio.sleep(0.3 * (attempt + 1))
    return None

async def worker(name, shard):
    timeout = aiohttp.ClientTimeout(total=4)
    cookie_hdr = {"Cookie": COOKIE}
    async with aiohttp.ClientSession(headers={**HEADERS, **cookie_hdr}, timeout=timeout) as session:
        while True:
            t0 = time.time()
            tasks = [fetch_one(session, cid, url) for cid, url in shard]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            now = int(time.time())
            for (cid, _), img in zip(shard, results):
                if not img:
                    continue
                h = phash_stub(img)
                # dedup cache check (Redis/Memcached) → skip nếu trùng gần
                # upload S3: put_object(bucket, key=f"{cid}/{now}.jpg", Body=img)
                # push Kafka metadata: {camera_id: cid, ts: now, key: s3_key, phash: h, size: len(img)}
            # ngủ phần còn lại của 5 giây
            dt = time.time() - t0
            await asyncio.sleep(max(0, PERIOD_SEC - dt + random.uniform(0, 0.15)))

async def main():
    # chia 700 camera cho N worker
    N = 20
    shards = [CAMERAS[i::N] for i in range(N)]
    await asyncio.gather(*(worker(f"W{i}", shards[i]) for i in range(N)))

if __name__ == "__main__":
    asyncio.run(main())
