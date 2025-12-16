import argparse
from pyspark.sql import SparkSession

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)  # hdfs:///... (dir)
    ap.add_argument("--tmp", required=True)    # hdfs:///... (dir)
    ap.add_argument("--final", required=True)  # hdfs:///... (file)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("ParquetDirToOneCSV").getOrCreate()
    sc = spark.sparkContext

    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    Path = jvm.org.apache.hadoop.fs.Path

    def to_path(hdfs_uri: str):
        return Path(hdfs_uri.replace("hdfs://", ""))

    tmp_path = to_path(args.tmp)
    final_path = to_path(args.final)

    parent = final_path.getParent()
    if not fs.exists(parent):
        fs.mkdirs(parent)

    if fs.exists(tmp_path):
        fs.delete(tmp_path, True)

    df = spark.read.parquet(args.input)

    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(args.tmp))

    part_file = None
    for st in fs.listStatus(tmp_path):
        name = st.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            part_file = st.getPath()
            break

    if part_file is None:
        raise RuntimeError("No part-*.csv found in tmp output dir")

    if fs.exists(final_path):
        fs.delete(final_path, False)

    if not fs.rename(part_file, final_path):
        raise RuntimeError("Failed to rename part file to final path")

    fs.delete(tmp_path, True)
    print("[OK] Wrote:", args.final)

    spark.stop()

if __name__ == "__main__":
    main()
