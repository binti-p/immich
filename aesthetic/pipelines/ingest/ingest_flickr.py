"""
Flickr-AES bootstrap ingestion for Aesthetic Hub.

Downloads Flickr-AES, computes CLIP ViT-L/14 embeddings (768-dim),
normalizes scores, splits by worker (user), and uploads to MinIO as
the permanent bootstrap dataset at:

  aesthetic-hub-data/
    datasets/personalized-flickr/
      train.parquet
      val.parquet
      test.parquet
      new_user_holdout.parquet
      dataset_card.json
    raw-data/flickr-aes/
      FLICKR-AES_image_score.txt
      FLICKR-AES_image_labeled_by_each_worker.csv

Mirrors ingestion/normalize.py + ingestion/assemble.py from the mlops repo,
adapted to write to MinIO instead of Chameleon object store.

Usage (inside aesthetic_service container or local with deps):
    python -m pipelines.ingest.ingest_flickr

Env vars:
    FLICKR_DIR          local path to store images (default: /tmp/flickr-aes)
    AWS_ENDPOINT_URL    MinIO endpoint (default: http://immich_minio:9000)
    AWS_ACCESS_KEY_ID   (default: minioadmin)
    AWS_SECRET_ACCESS_KEY (default: minioadmin)
    MINIO_BUCKET        (default: aesthetic-hub-data)
    SKIP_DOWNLOAD       set to "true" to skip download if already present
    SKIP_UPLOAD         set to "true" to only write locally
"""
import io
import json
import logging
import os
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
FLICKR_DIR        = Path(os.environ.get("FLICKR_DIR", "/tmp/flickr-aes"))
OUTPUT_DIR        = Path("/tmp/aesthetic-hub-flickr-output")
CACHE_FILE        = Path("/tmp/flickr-clip-cache.npy")
FLICKR_FOLDER_ID  = "1LR6trJhN4XbgTtqZo1zfe272cAkXqA7e"

MINIO_ENDPOINT    = os.environ.get("AWS_ENDPOINT_URL", "http://immich_minio:9000")
MINIO_KEY         = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET      = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET            = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")

SKIP_DOWNLOAD     = os.environ.get("SKIP_DOWNLOAD", "false").lower() == "true"
SKIP_UPLOAD       = os.environ.get("SKIP_UPLOAD", "false").lower() == "true"

SEED              = 42
BATCH_SIZE        = 64
CLIP_DIM          = 768

# Split ratios — mirrors ingestion/normalize.py
SEEN_IMAGE_RATIOS     = {"train": 0.70, "val": 0.10, "test": 0.10, "production_seen": 0.10}
NEW_USER_WORKER_RATIO = 0.20


def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


# ── Download ──────────────────────────────────────────────────────────────────

def download_flickr():
    if SKIP_DOWNLOAD and (FLICKR_DIR / "40K").exists():
        log.info("SKIP_DOWNLOAD=true and 40K/ exists, skipping download")
        return

    if (FLICKR_DIR / "40K").exists():
        log.info("Flickr-AES already downloaded, skipping")
        return

    log.info("Downloading Flickr-AES via gdown...")
    FLICKR_DIR.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        f"gdown --folder {FLICKR_FOLDER_ID} -O {FLICKR_DIR}/",
        shell=True,
    )
    if result.returncode != 0:
        raise RuntimeError("gdown failed — install with: pip install gdown")

    # Flatten any subdirectories
    for subdir in [d for d in FLICKR_DIR.iterdir() if d.is_dir() and d.name != "40K"]:
        for f in subdir.iterdir():
            f.rename(FLICKR_DIR / f.name.strip())
        subdir.rmdir()

    # Unzip image archive
    zips = [f for f in FLICKR_DIR.glob("*.zip") if "FLICKR" in f.name.upper()]
    if zips:
        log.info(f"Unzipping {zips[0].name}...")
        with zipfile.ZipFile(zips[0]) as z:
            z.extractall(FLICKR_DIR)
        for z in FLICKR_DIR.glob("*.zip"):
            z.unlink()

    n_images = len(list((FLICKR_DIR / "40K").glob("*.jpg")))
    log.info(f"Download complete: {n_images} images")


# ── Normalize — mirrors ingestion/normalize.py ────────────────────────────────

def split_items(items, ratios, seed=42):
    items = np.array(sorted(list(items)))
    rng   = np.random.default_rng(seed)
    items = items[rng.permutation(len(items))]
    names = list(ratios.keys())
    vals  = np.array(list(ratios.values()), dtype=float)
    vals /= vals.sum()
    counts = np.floor(vals * len(items)).astype(int)
    counts[-1] = len(items) - counts[:-1].sum()
    splits, start = {}, 0
    for name, count in zip(names, counts):
        splits[name] = items[start:start + count]
        start += count
    return splits


def load_flickr_scores() -> pd.DataFrame:
    rows = []
    with open(FLICKR_DIR / "FLICKR-AES_image_score.txt", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2 and parts[0].lower().endswith(".jpg"):
                try:
                    rows.append((parts[0], float(parts[1])))
                except ValueError:
                    continue
    df = pd.DataFrame(rows, columns=["image_name", "global_score"])
    return df.drop_duplicates(subset=["image_name"]).reset_index(drop=True)


def load_flickr_workers() -> pd.DataFrame:
    df = pd.read_csv(
        FLICKR_DIR / "FLICKR-AES_image_labeled_by_each_worker.csv",
        skipinitialspace=True,
    )
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "imagePair": "image_name",
        "worker":    "worker_id",
        "score":     "worker_score",
    })
    # Normalize worker scores (1-5) → (0-1), mirrors ingestion/normalize.py
    df["label"] = (df["worker_score"].astype(float) - 1.0) / 4.0
    return df


def build_splits(workers_df: pd.DataFrame) -> pd.DataFrame:
    """Assign train/val/test/production splits by worker, mirrors normalize.py."""
    image_lookup = {p.name: p for p in (FLICKR_DIR / "40K").glob("*.jpg")}
    workers_df["image_path"] = workers_df["image_name"].map(image_lookup)
    workers_df = workers_df[workers_df["image_path"].notna()].reset_index(drop=True)

    all_workers = np.array(sorted(workers_df["worker_id"].unique()))
    rng = np.random.default_rng(SEED)
    perm = rng.permutation(len(all_workers))
    n_holdout = max(1, int(round(NEW_USER_WORKER_RATIO * len(all_workers))))
    new_user_workers = set(all_workers[perm[:n_holdout]])
    seen_workers     = set(all_workers[perm[n_holdout:]])

    seen_df = workers_df[workers_df["worker_id"].isin(seen_workers)]
    seen_split_map = {
        img: s
        for s, imgs in split_items(seen_df["image_name"].unique(), SEEN_IMAGE_RATIOS, SEED).items()
        for img in imgs
    }

    mask = workers_df["worker_id"].isin(seen_workers)
    workers_df["split"] = None
    workers_df.loc[mask,  "split"] = workers_df.loc[mask,  "image_name"].map(seen_split_map)
    workers_df.loc[~mask, "split"] = "new_user_holdout"

    log.info(f"Splits: {workers_df['split'].value_counts().to_dict()}")
    log.info(f"Seen workers: {len(seen_workers)}, holdout: {len(new_user_workers)}")
    return workers_df


# ── CLIP embeddings — mirrors ingestion/assemble.py ───────────────────────────

def load_cache() -> dict:
    if CACHE_FILE.exists():
        cache = np.load(CACHE_FILE, allow_pickle=True).item()
        log.info(f"Resuming from cache: {len(cache)} embeddings")
        return cache
    return {}


def save_cache(cache: dict):
    np.save(CACHE_FILE, cache)


def compute_embeddings(image_paths: list[Path], cache: dict) -> dict:
    try:
        import torch
        import clip
        from PIL import Image, ImageFile
        ImageFile.LOAD_TRUNCATED_IMAGES = True
    except ImportError:
        raise ImportError("Install: pip install torch torchvision git+https://github.com/openai/CLIP.git")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info(f"Loading CLIP ViT-L/14 on {device} (first run downloads ~933MB)...")
    model, preprocess = clip.load("ViT-L/14", device=device)
    model.eval()

    missing = [p for p in image_paths if str(p) not in cache]
    log.info(f"{len(image_paths)} images total, {len(missing)} need embedding")

    saved_at = 0
    try:
        from tqdm import tqdm
        iterator = tqdm(range(0, len(missing), BATCH_SIZE), desc="embedding")
    except ImportError:
        iterator = range(0, len(missing), BATCH_SIZE)

    with torch.no_grad():
        for start in iterator:
            batch = missing[start:start + BATCH_SIZE]
            tensors, valid = [], []
            for p in batch:
                try:
                    tensors.append(preprocess(Image.open(p).convert("RGB")))
                    valid.append(p)
                except Exception:
                    pass
            if not tensors:
                continue
            feats = model.encode_image(torch.stack(tensors).to(device))
            feats = (feats / feats.norm(dim=-1, keepdim=True)).cpu().numpy().astype(np.float32)
            for path, emb in zip(valid, feats):
                cache[str(path)] = emb
            if len(cache) - saved_at >= 500:
                save_cache(cache)
                saved_at = len(cache)

    save_cache(cache)
    return cache


# ── Assemble parquets ─────────────────────────────────────────────────────────

# Schema matches pipeline.py PARQUET_SCHEMA
PARQUET_SCHEMA = pa.schema([
    pa.field("user_id",        pa.string()),
    pa.field("asset_id",       pa.string()),   # image_name used as asset_id for bootstrap
    pa.field("clip_embedding", pa.list_(pa.float32(), CLIP_DIM)),
    pa.field("label",          pa.float32()),
    pa.field("event_type",     pa.string()),   # "flickr_annotation" for bootstrap data
])


def build_parquet_df(df: pd.DataFrame, cache: dict) -> pd.DataFrame:
    """Add embeddings, drop missing, rename columns to match our schema."""
    df = df.copy()
    df["clip_embedding"] = df["image_path"].apply(lambda p: cache.get(str(p)))
    df = df.dropna(subset=["clip_embedding"]).reset_index(drop=True)
    df["event_type"] = "flickr_annotation"
    df = df.rename(columns={"worker_id": "user_id", "image_name": "asset_id"})
    return df[["user_id", "asset_id", "clip_embedding", "label", "event_type"]]


def write_parquet_local(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "user_id":        df["user_id"].tolist(),
            "asset_id":       df["asset_id"].tolist(),
            "clip_embedding": [list(e) for e in df["clip_embedding"]],
            "label":          df["label"].astype(np.float32).tolist(),
            "event_type":     df["event_type"].tolist(),
        },
        schema=PARQUET_SCHEMA,
    )
    pq.write_table(table, str(path), compression="snappy")
    log.info(f"Wrote {len(df):,} rows → {path}")
    return table


# ── Upload to MinIO ───────────────────────────────────────────────────────────

def upload_file(client, local_path: Path, key: str):
    with open(local_path, "rb") as f:
        client.upload_fileobj(f, BUCKET, key)
    log.info(f"Uploaded s3://{BUCKET}/{key}")


def upload_json(client, data: dict, key: str):
    client.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode(),
        ContentType="application/json",
    )
    log.info(f"Uploaded s3://{BUCKET}/{key}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Download
    download_flickr()

    # 2. Load + normalize
    log.info("Loading Flickr-AES scores and worker annotations...")
    workers_df = load_flickr_workers()
    workers_df = build_splits(workers_df)

    # 3. Compute CLIP embeddings
    image_paths = [p for p in (FLICKR_DIR / "40K").glob("*.jpg")]
    cache = load_cache()
    cache = compute_embeddings(image_paths, cache)

    # 4. Assemble parquets per split
    split_stats = {}
    for split in ["train", "val", "test", "new_user_holdout"]:
        split_df = workers_df[workers_df["split"] == split]
        if split_df.empty:
            log.warning(f"No rows for split={split}, skipping")
            continue
        out_df = build_parquet_df(split_df, cache)
        out_path = OUTPUT_DIR / f"{split}.parquet"
        write_parquet_local(out_df, out_path)
        split_stats[split] = {
            "users": int(out_df["user_id"].nunique()),
            "rows":  len(out_df),
        }

    # 5. Write dataset_card.json
    dataset_card = {
        "version":     "bootstrap",
        "created_at":  datetime.now(timezone.utc).isoformat(),
        "source":      "FLICKR-AES (Ren et al., 2017)",
        "clip_model":  "ViT-L/14",
        "clip_dim":    CLIP_DIM,
        "label_description": "per-worker score normalized (worker_score - 1) / 4 → [0, 1]",
        "split_basis": "80% seen workers / 20% holdout new users, seed=42",
        "leakage_controls": [
            "holdout users (20% of annotators) excluded from train/val/test",
            "splits by worker — no worker appears in both train and val",
        ],
        "splits": split_stats,
        "schema": {
            "columns":   ["user_id", "asset_id", "clip_embedding", "label", "event_type"],
            "clip_dim":  CLIP_DIM,
            "event_type_value": "flickr_annotation",
        },
    }
    card_path = OUTPUT_DIR / "dataset_card.json"
    with open(card_path, "w") as f:
        json.dump(dataset_card, f, indent=2)
    log.info(f"Wrote {card_path}")

    # 6. Upload to MinIO
    if SKIP_UPLOAD:
        log.info("SKIP_UPLOAD=true, skipping MinIO upload")
        log.info(f"Output files in {OUTPUT_DIR}")
        return

    client = s3()
    prefix = "datasets/personalized-flickr"

    for split in ["train", "val", "test", "new_user_holdout"]:
        local = OUTPUT_DIR / f"{split}.parquet"
        if local.exists():
            upload_file(client, local, f"{prefix}/{split}.parquet")

    upload_json(client, dataset_card, f"{prefix}/dataset_card.json")

    # Upload raw metadata files
    for fname in ["FLICKR-AES_image_score.txt", "FLICKR-AES_image_labeled_by_each_worker.csv"]:
        src = FLICKR_DIR / fname
        if src.exists():
            upload_file(client, src, f"raw-data/flickr-aes/{fname}")

    log.info("Ingestion complete.")
    log.info(f"Bootstrap dataset at s3://{BUCKET}/{prefix}/")


if __name__ == "__main__":
    main()
