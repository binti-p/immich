import os
import numpy as np
import tritonclient.http as triton_http

TRITON_HOST = os.environ.get("TRITON_HOST", "triton-production")
TRITON_PORT = os.environ.get("TRITON_PORT", "8000")
TRITON_URL = f"{TRITON_HOST}:{TRITON_PORT}"


def get_client():
    return triton_http.InferenceServerClient(url=TRITON_URL)


def infer_global(clip_embedding: np.ndarray) -> float:
    """
    Calls global_mlp model.
    Input: input [768] float32
    """
    client = get_client()

    inp = triton_http.InferInput("input", [1, 768], "FP32")
    inp.set_data_from_numpy(clip_embedding.reshape(1, 768))

    out = triton_http.InferRequestedOutput("output")
    result = client.infer("global_mlp", inputs=[inp], outputs=[out])
    return float(result.as_numpy("output")[0][0])


def infer_personalized(clip_embedding: np.ndarray, user_embedding: np.ndarray) -> float:
    """
    Calls personalized_mlp model.
    Inputs: image_embedding [768] float32, user_embedding [64] float32
    Model concatenates them internally.
    """
    client = get_client()

    inp_clip = triton_http.InferInput("image_embedding", [1, 768], "FP32")
    inp_clip.set_data_from_numpy(clip_embedding.reshape(1, 768))

    inp_user = triton_http.InferInput("user_embedding", [1, 64], "FP32")
    inp_user.set_data_from_numpy(user_embedding.reshape(1, 64))

    out = triton_http.InferRequestedOutput("output")
    result = client.infer(
        "personalized_mlp",
        inputs=[inp_clip, inp_user],
        outputs=[out]
    )
    return float(result.as_numpy("output")[0][0])