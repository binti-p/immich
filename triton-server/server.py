"""
Triton HTTP inference protocol (KServe v2) compatible server using ONNX Runtime.
Handles both:
  - Pure JSON requests (for testing)
  - Binary-prefixed requests from tritonclient.http (Inference-Header-Content-Length header)

Models served: global_mlp, personalized_mlp
"""
import os
import json
import struct
import numpy as np
import onnxruntime as ort
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from typing import Any

MODEL_REPO = os.environ.get("MODEL_REPOSITORY", "/models")

# ── Load models ──────────────────────────────────────────────────────────────

sessions: dict[str, ort.InferenceSession] = {}

def load_models():
    for model_name in ["global_mlp", "personalized_mlp"]:
        model_path = os.path.join(MODEL_REPO, model_name, "1", "model.onnx")
        if os.path.exists(model_path):
            sessions[model_name] = ort.InferenceSession(
                model_path, providers=["CPUExecutionProvider"]
            )
            print(f"[startup] Loaded model: {model_name}")
        else:
            print(f"[startup] WARNING: Model not found at {model_path}")

load_models()

app = FastAPI(title="Triton-compatible Inference Server")

DTYPE_MAP = {
    "FP32": np.float32,
    "FP16": np.float16,
    "INT32": np.int32,
    "INT64": np.int64,
    "BYTES": np.object_,
}


# ── Health endpoints ──────────────────────────────────────────────────────────

@app.get("/v2/health/live")
def health_live():
    return {"live": True}

@app.get("/v2/health/ready")
def health_ready():
    if not sessions:
        raise HTTPException(status_code=503, detail="No models loaded")
    return {"ready": True}

@app.get("/v2")
def server_metadata():
    return {"name": "triton-compat", "version": "1.0.0", "extensions": []}

@app.get("/v2/models/{model_name}/ready")
def model_ready(model_name: str):
    if model_name not in sessions:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    return {"name": model_name, "ready": True}

@app.get("/v2/models/{model_name}/versions/{version}/ready")
def model_version_ready(model_name: str, version: str):
    return model_ready(model_name)

@app.get("/v2/models/{model_name}")
def model_metadata(model_name: str):
    if model_name not in sessions:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    sess = sessions[model_name]
    inputs = [{"name": i.name, "shape": [-1] + list(i.shape[1:]), "datatype": "FP32"} for i in sess.get_inputs()]
    outputs = [{"name": o.name, "shape": [-1, 1], "datatype": "FP32"} for o in sess.get_outputs()]
    return {"name": model_name, "versions": ["1"], "platform": "onnxruntime_onnx",
            "inputs": inputs, "outputs": outputs}


# ── Inference endpoint ────────────────────────────────────────────────────────

def _run_inference(model_name: str, infer_request: dict) -> dict:
    """Core inference logic shared by JSON and binary paths."""
    sess = sessions[model_name]
    feed = {}

    for inp in infer_request.get("inputs", []):
        dtype = DTYPE_MAP.get(inp["datatype"], np.float32)
        shape = inp["shape"]
        # data may be nested list or flat list
        data = inp.get("data", [])
        arr = np.array(data, dtype=dtype).reshape(shape)
        feed[inp["name"]] = arr

    outputs = sess.run(None, feed)
    output_metas = sess.get_outputs()

    response_outputs = []
    for meta, arr in zip(output_metas, outputs):
        response_outputs.append({
            "name": meta.name,
            "shape": list(arr.shape),
            "datatype": "FP32",
            "data": arr.flatten().tolist()
        })

    return {"model_name": model_name, "model_version": "1", "outputs": response_outputs}


@app.post("/v2/models/{model_name}/infer")
async def infer(model_name: str, request: Request):
    if model_name not in sessions:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    body = await request.body()
    header_len = request.headers.get("inference-header-content-length")

    if header_len is not None:
        # Binary protocol: first `header_len` bytes are JSON, rest is binary tensor data
        json_len = int(header_len)
        json_bytes = body[:json_len]
        binary_data = body[json_len:]
        infer_request = json.loads(json_bytes)

        # Decode binary tensors — each input's binary data is appended in order
        offset = 0
        for inp in infer_request.get("inputs", []):
            if inp.get("parameters", {}).get("binary_data_size"):
                size = inp["parameters"]["binary_data_size"]
                dtype = DTYPE_MAP.get(inp["datatype"], np.float32)
                arr = np.frombuffer(binary_data[offset:offset + size], dtype=dtype)
                inp["data"] = arr.tolist()
                offset += size
    else:
        # Pure JSON protocol
        try:
            infer_request = json.loads(body)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

    try:
        result = _run_inference(model_name, infer_request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse(content=result)


@app.post("/v2/models/{model_name}/versions/{version}/infer")
async def infer_versioned(model_name: str, version: str, request: Request):
    return await infer(model_name, request)
