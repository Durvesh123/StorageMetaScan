#!/usr/bin/env python3
"""
scan.py - small POC scanner that enumerates files, collects metadata,
computes partial hashes for candidates and writes Parquet output.

Usage:
    python3 scripts/scan.py --config config.yaml
"""

import os
import argparse
import yaml
import logging
from datetime import date, datetime
from pathlib import Path

# PySpark imports
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F  # <-- FIXED: import functions correctly

# helpers (partial/full hashing)
from scripts.helpers import safe_stat, partial_hash, full_hash

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("storage_monitor.scan")

# ----------------------
# Helper functions
# ----------------------
def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def enumerate_top_dirs(root: str, depth_limit: int = 1):
    root = os.path.abspath(root)
    results = []
    if os.path.isfile(root):
        return [os.path.dirname(root)]
    for dirpath, dirnames, _ in os.walk(root):
        results.append(dirpath)
        if dirpath.count(os.sep) - root.count(os.sep) >= depth_limit:
            dirnames[:] = []
    return results

def file_metadata_row(path, cfg):
    st = safe_stat(path)
    if st is None:
        return None
    try:
        owner, group = None, None
        try:
            import pwd, grp
            owner = pwd.getpwuid(st.st_uid).pw_name
            group = grp.getgrgid(st.st_gid).gr_name
        except Exception:
            owner, group = str(st.st_uid), str(st.st_gid)

        ext = Path(path).suffix.lower()
        depth = Path(path).parts.__len__() - Path(cfg_root).parts.__len__()

        row = {
            "path": path,
            "is_dir": False,
            "size_bytes": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime),
            "owner": owner,
            "group": group,
            "mode": oct(st.st_mode & 0o777),
            "extension": ext,
            "depth": depth,
            "parent_path": os.path.dirname(path),
            "partial_hash": None,
            "full_hash": None,
        }

        if st.st_size >= cfg.get("min_hash_size_bytes", 1024):
            row["partial_hash"] = partial_hash(path, sample_bytes=cfg.get("partial_hash_bytes", 4096))
        return row
    except Exception as e:
        log.debug("file_metadata_row failed for %s: %s", path, e)
        return None

def walk_and_collect(top_dir, cfg):
    results = []
    for dirpath, dirnames, filenames in os.walk(top_dir):
        for f in filenames:
            p = os.path.join(dirpath, f)
            meta = file_metadata_row(p, cfg)
            if meta:
                results.append(meta)
    return results

def compute_full_hashes_for_candidates(df_rows, cfg):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in df_rows:
        key = (r["size_bytes"], r.get("partial_hash"))
        groups[key].append(r)
    for key, rows in groups.items():
        if len(rows) <= 1:
            continue
        for r in rows:
            r["full_hash"] = full_hash(r["path"])
    return df_rows

# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", required=True, help="Path to config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    global cfg_root
    cfg_root = os.path.abspath(cfg["root_paths"][0])

    # Spark setup
    conf = SparkConf().setAppName("storage-monitor-poc").setMaster(f"local[{cfg.get('max_workers', 4)}]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    log.info("Starting scan for roots: %s", cfg["root_paths"])
    all_top_dirs = []
    for root in cfg["root_paths"]:
        top = enumerate_top_dirs(root, depth_limit=cfg.get("walk_chunk_depth", 1))
        all_top_dirs.extend(top)
    all_top_dirs = sorted(set(all_top_dirs))

    # parallelize
    num_parts = min(len(all_top_dirs), cfg.get("max_workers", 4)) or 1
    rdd = sc.parallelize(all_top_dirs, num_parts).flatMap(lambda td: walk_and_collect(td, cfg))

    rows = rdd.collect()
    log.info("Collected %d file rows (driver).", len(rows))

    updated_rows = compute_full_hashes_for_candidates(rows, cfg)

    df = spark.createDataFrame(updated_rows)
    scan_date = cfg.get("scan_date") if cfg.get("scan_date") != "auto" else date.today().isoformat()
    out_dir = os.path.join(cfg.get("output_dir", "./outputs/metadata"), f"scan-{scan_date}")
    os.makedirs(out_dir, exist_ok=True)

    # ✅ FIXED: Use correct pyspark.sql.functions import
    df = df.withColumn("scan_date", F.lit(scan_date))
    df.write.mode("overwrite").parquet(out_dir)

    log.info("Scan finished. Wrote metadata to %s", out_dir)
    spark.stop()

