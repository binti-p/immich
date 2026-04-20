"""
One-time script to generate an empty user_embeddings.parquet with the correct schema.
Run locally: python generate_empty_embeddings.py
Output: empty_embeddings.parquet (committed to repo, used by bucket-init)
"""
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([
    pa.field("user_id",   pa.string()),
    pa.field("embedding", pa.list_(pa.float32(), 64)),
])

table = pa.table({"user_id": [], "embedding": []}, schema=schema)
pq.write_table(table, "empty_embeddings.parquet")
print("Created empty_embeddings.parquet")
