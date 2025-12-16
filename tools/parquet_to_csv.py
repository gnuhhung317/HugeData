import argparse
from pathlib import Path
import pandas as pd

def parquet_to_csv(input_path: str, output_csv: str):
    in_path = Path(input_path)
    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if in_path.is_dir():
        files = sorted(in_path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No .parquet found in {in_path}")
        first = True
        for f in files:
            df = pd.read_parquet(f)  # requires pyarrow
            df.to_csv(out_path, index=False, mode="w" if first else "a", header=first, encoding="utf-8-sig")
            first = False
    else:
        df = pd.read_parquet(in_path)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] Saved CSV: {out_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Parquet file OR folder containing *.parquet")
    ap.add_argument("--output", required=True, help="Output CSV path")
    args = ap.parse_args()
    parquet_to_csv(args.input, args.output)

if __name__ == "__main__":
    main()
