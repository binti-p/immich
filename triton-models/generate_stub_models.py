#!/usr/bin/env python3
"""
Generates stub ONNX models for local Triton testing.
These use random weights — replace with real trained models for production.

Usage:
    pip install torch onnx numpy
    python generate_stub_models.py
"""
import numpy as np
import os

try:
    import torch
    import torch.nn as nn
    import onnx

    class GlobalMLP(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(768, 256), nn.ReLU(),
                nn.Linear(256, 64),  nn.ReLU(),
                nn.Linear(64, 1),    nn.Sigmoid()
            )
        def forward(self, x):
            return self.net(x)

    class PersonalizedMLP(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(832, 256), nn.ReLU(),
                nn.Linear(256, 64),  nn.ReLU(),
                nn.Linear(64, 1),    nn.Sigmoid()
            )
        def forward(self, image_embedding, user_embedding):
            x = torch.cat([image_embedding, user_embedding], dim=-1)
            return self.net(x)

    # Global MLP
    global_model = GlobalMLP()
    global_model.eval()
    dummy_input = torch.randn(1, 768)
    torch.onnx.export(
        global_model, dummy_input,
        "global_mlp/1/model.onnx",
        input_names=["input"],
        output_names=["output"],
        dynamic_axes={"input": {0: "batch"}, "output": {0: "batch"}},
        opset_version=17
    )
    print("✓ global_mlp/1/model.onnx created")

    # Personalized MLP
    pers_model = PersonalizedMLP()
    pers_model.eval()
    dummy_clip = torch.randn(1, 768)
    dummy_user = torch.randn(1, 64)
    torch.onnx.export(
        pers_model, (dummy_clip, dummy_user),
        "personalized_mlp/1/model.onnx",
        input_names=["image_embedding", "user_embedding"],
        output_names=["output"],
        dynamic_axes={
            "image_embedding": {0: "batch"},
            "user_embedding":  {0: "batch"},
            "output":          {0: "batch"}
        },
        opset_version=17
    )
    print("✓ personalized_mlp/1/model.onnx created")
    print("\nStub models generated. Replace with real trained weights for production.")

except ImportError:
    print("torch/onnx not installed. Installing...")
    os.system("pip install torch onnx --quiet")
    print("Re-run this script after installation.")
