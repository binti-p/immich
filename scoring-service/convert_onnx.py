import os
import argparse
import json
import torch
import torch.nn as nn
import onnx
import onnxruntime as ort
import numpy as np


class PersonalizedMLP(nn.Module):
    def __init__(self, input_dim=832):
        """
        Takes pre-concatenated (clip_embedding[768] + user_embedding[64]) = 832-dim input.
        No internal embedding lookup — user embedding is passed in directly.
        """
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 512), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(512, 128),       nn.ReLU(), nn.Dropout(0.1),
            nn.Linear(128, 32),        nn.ReLU(),
            nn.Linear(32, 1),
        )

    def forward(self, image_embedding, user_embedding):
        x = torch.cat([image_embedding, user_embedding], dim=-1)
        return torch.sigmoid(self.net(x))


def convert_ckpt_to_optimized_onnx(ckpt_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # --- 1. Load checkpoint ---
    ckpt = torch.load(ckpt_path, map_location="cpu", weights_only=False)
    state_dict = ckpt["model_state_dict"]
    # user2idx no longer needed — embeddings passed directly
    print(f"[1/4] Loaded checkpoint")

    # --- 2. Save inference-only sidecar ---
    inference_only_path = os.path.join(output_dir, "flickr_personalized_inference_only.pth")
    torch.save({"state_dict": state_dict}, inference_only_path)
    print(f"[2/4] Inference-only .pth saved")

    # --- 3. Export base ONNX ---
    model = PersonalizedMLP()
    model.load_state_dict(state_dict)
    model.eval()

    base_onnx_path = os.path.join(output_dir, "flickr_personalized.onnx")
    dummy_clip = torch.randn(1, 768)
    dummy_user = torch.randn(1, 64)

    torch.onnx.export(
        model,
        (dummy_clip, dummy_user),
        base_onnx_path,
        export_params=True,
        opset_version=17,
        do_constant_folding=True,
        input_names=["image_embedding", "user_embedding"],
        output_names=["output"],
        dynamic_axes={
            "image_embedding": {0: "batch_size"},
            "user_embedding":  {0: "batch_size"},
            "output":          {0: "batch_size"},
        },
    )

    model_proto = onnx.load(base_onnx_path)
    onnx.save(model_proto, base_onnx_path,
              save_as_external_data=False, all_tensors_to_one_file=True)
    onnx.checker.check_model(onnx.load(base_onnx_path))
    print(f"[3/4] Base ONNX saved ({os.path.getsize(base_onnx_path)/1e6:.2f} MB)")

    # --- 4. Graph optimization ---
    optimized_onnx_path = os.path.join(output_dir, "flickr_personalized_optimized.onnx")
    sess_opts = ort.SessionOptions()
    sess_opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_EXTENDED
    sess_opts.optimized_model_filepath = optimized_onnx_path
    ort.InferenceSession(base_onnx_path, sess_opts=sess_opts,
                         providers=["CPUExecutionProvider"])
    print(f"[4/4] Optimized ONNX saved ({os.path.getsize(optimized_onnx_path)/1e6:.2f} MB)")

    # --- Sanity check ---
    sess = ort.InferenceSession(optimized_onnx_path,
                                providers=["CPUExecutionProvider"])
    test_clip = np.random.randn(1, 768).astype(np.float32)
    test_user = np.random.randn(1, 64).astype(np.float32)
    score = sess.run(None, {
        "image_embedding": test_clip,
        "user_embedding": test_user
    })[0]
    assert 0.0 <= score.flatten()[0] <= 1.0
    print(f"Sanity check passed: score={score.flatten()[0]:.4f} ✓")

    return {
        "inference_only_pth": inference_only_path,
        "base_onnx": base_onnx_path,
        "optimized_onnx": optimized_onnx_path,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ckpt-path", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    result = convert_ckpt_to_optimized_onnx(
        ckpt_path=args.ckpt_path,
        output_dir=args.output_dir
    )
    print(f"\nConversion complete: {result['optimized_onnx']}")