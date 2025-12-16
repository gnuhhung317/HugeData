import argparse
import csv
from datetime import datetime
from pathlib import Path
import requests


def webhdfs_mkdirs(namenode: str, hdfs_dir: str, user: str):
    namenode = namenode.rstrip("/")
    url = f"{namenode}/webhdfs/v1{hdfs_dir}?op=MKDIRS&user.name={user}"
    r = requests.put(url, timeout=30)
    r.raise_for_status()


def webhdfs_upload_file(namenode: str, local_path: Path, hdfs_path: str, user: str, overwrite: bool = True):
    namenode = namenode.rstrip("/")
    overwrite_str = "true" if overwrite else "false"

    # Step 1: request CREATE -> nhận 307 redirect tới DataNode
    create_url = f"{namenode}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite={overwrite_str}&user.name={user}"
    r1 = requests.put(create_url, allow_redirects=False, timeout=30)

    if r1.status_code == 201:
        return  # some setups may directly create
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE failed: {r1.status_code} {r1.text}")

    location = r1.headers.get("Location")
    if not location:
        raise RuntimeError("WebHDFS did not return redirect Location header.")

    # Step 2: PUT file bytes to redirected URL
    with local_path.open("rb") as f:
        r2 = requests.put(location, data=f, timeout=120)
        r2.raise_for_status()


def write_sample_csv(out_file: Path):
    # Bạn thay schema/fields theo data traffic thật của bạn
    rows = [
        {
            "ts": datetime.utcnow().isoformat(),
            "camera_id": "cam_01",
            "location": "nga_tu_A",
            "vehicle_count": 12,
            "avg_speed": 28.5,
        },
        {
            "ts": datetime.utcnow().isoformat(),
            "camera_id": "cam_02",
            "location": "nga_tu_B",
            "vehicle_count": 7,
            "avg_speed": 31.2,
        },
    ]
    fieldnames = list(rows[0].keys())
    out_file.parent.mkdir(parents=True, exist_ok=True)

    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namenode", default="http://localhost:9870", help="NameNode HTTP, bạn đang port-forward nên dùng localhost")
    p.add_argument("--user", default="spark", help="HDFS user.name")
    p.add_argument("--hdfs-dir", default="/traffic_data/csv", help="Thư mục đích trên HDFS")
    p.add_argument("--out-dir", default="data/csv_archive", help="Thư mục lưu CSV vĩnh viễn trên máy")
    p.add_argument("--input", default=None, help="Nếu đã có file CSV sẵn thì truyền đường dẫn vào đây (không tạo sample)")
    args = p.parse_args()

    if args.input:
        local_csv = Path(args.input).resolve()
        if not local_csv.exists():
            raise FileNotFoundError(local_csv)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_csv = Path(args.out_dir) / f"traffic_{ts}.csv"
        write_sample_csv(local_csv)

    webhdfs_mkdirs(args.namenode, args.hdfs_dir, args.user)

    hdfs_path = f"{args.hdfs_dir.rstrip('/')}/{local_csv.name}"
    webhdfs_upload_file(args.namenode, local_csv, hdfs_path, args.user, overwrite=True)

    print(f"[OK] Local saved: {local_csv}")
    print(f"[OK] Uploaded to HDFS: {hdfs_path}")
    print("Check on UI: http://localhost:9870 -> Utilities/Browse ->", args.hdfs_dir)


if __name__ == "__main__":
    main()
