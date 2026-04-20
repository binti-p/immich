from __future__ import annotations

import json
import random
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
import yaml
from scipy.stats import pearsonr, spearmanr
from torch.utils.data import DataLoader, Dataset


def load_config(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def ensure_dirs(*paths: str | Path) -> None:
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


class ManifestEmbeddingDataset(Dataset):
    def __init__(self, manifest_df: pd.DataFrame, user2idx: dict[str, int]):
        self.df = manifest_df.reset_index(drop=True).copy()
        self.user2idx = user2idx

        if self.df.empty:
            self.embs = torch.zeros((0, 768), dtype=torch.float32)
            self.user_idxs = torch.zeros((0,), dtype=torch.long)
            self.targets = torch.zeros((0, 1), dtype=torch.float32)
            self.asset_ids: list[str] = []
            self.user_ids: list[str] = []
            self.splits: list[str] = []
            return

        self.embs = torch.tensor(np.stack(self.df["clip_embedding"].to_list()), dtype=torch.float32)
        self.user_idxs = torch.tensor([self.user2idx.get(user_id, 0) for user_id in self.df["user_id"]], dtype=torch.long)
        self.targets = torch.tensor(self.df["label"].astype(float).to_numpy(), dtype=torch.float32).unsqueeze(1)
        self.asset_ids = self.df["asset_id"].tolist()
        self.user_ids = self.df["user_id"].tolist()
        self.splits = self.df["split"].tolist()

    def __len__(self) -> int:
        return len(self.df)

    def __getitem__(self, idx: int) -> dict:
        return {
            "emb": self.embs[idx],
            "user_idx": self.user_idxs[idx],
            "target": self.targets[idx],
            "asset_id": self.asset_ids[idx],
            "user_id": self.user_ids[idx],
            "split": self.splits[idx],
        }


def collate_personalized(batch: list[dict]) -> dict:
    return {
        "emb": torch.stack([item["emb"] for item in batch], dim=0),
        "user_idx": torch.stack([item["user_idx"] for item in batch], dim=0),
        "target": torch.stack([item["target"] for item in batch], dim=0),
        "asset_id": [item["asset_id"] for item in batch],
        "user_id": [item["user_id"] for item in batch],
        "split": [item["split"] for item in batch],
    }


class PersonalizedMLP(nn.Module):
    def __init__(
        self,
        num_users: int,
        input_dim: int = 768,
        user_emb_dim: int = 64,
    ):
        super().__init__()
        self.user_embedding = nn.Embedding(num_users, user_emb_dim, padding_idx=0)
        combined_dim = input_dim + user_emb_dim
        self.net = nn.Sequential(
            nn.Linear(combined_dim, 512),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(512, 128),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(128, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )
        with torch.no_grad():
            self.user_embedding.weight[0].zero_()

    def forward(self, image_emb: torch.Tensor, user_idx: torch.Tensor) -> torch.Tensor:
        user_emb = self.user_embedding(user_idx)
        return torch.sigmoid(self.net(torch.cat([image_emb, user_emb], dim=-1)))


def train_one_epoch_personalized(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    device: str,
) -> float:
    model.train()
    losses: list[float] = []

    for batch in loader:
        image_emb = batch["emb"].to(device).float()
        user_idx = batch["user_idx"].to(device)
        targets = batch["target"].to(device).float()

        optimizer.zero_grad(set_to_none=True)
        predictions = model(image_emb, user_idx)
        loss = F.mse_loss(predictions, targets)
        loss.backward()
        optimizer.step()
        losses.append(loss.item())

    return float(np.mean(losses)) if losses else float("nan")


def _safe_corr(fn, y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_true) < 2 or np.allclose(y_true, y_true[0]) or np.allclose(y_pred, y_pred[0]):
        return float("nan")
    value, _ = fn(y_true, y_pred)
    return float(value)


def evaluate_personalized(model: nn.Module, loader: DataLoader, device: str) -> tuple[dict, pd.DataFrame]:
    model.eval()
    predictions: list[float] = []
    targets: list[float] = []
    asset_ids: list[str] = []
    user_ids: list[str] = []
    splits: list[str] = []

    with torch.no_grad():
        for batch in loader:
            image_emb = batch["emb"].to(device).float()
            user_idx = batch["user_idx"].to(device)
            pred = model(image_emb, user_idx).cpu().numpy().reshape(-1)

            predictions.extend(pred.tolist())
            targets.extend(batch["target"].cpu().numpy().reshape(-1).tolist())
            asset_ids.extend(batch["asset_id"])
            user_ids.extend(batch["user_id"])
            splits.extend(batch["split"])

    pred_np = np.asarray(predictions, dtype=np.float32)
    target_np = np.asarray(targets, dtype=np.float32)
    if len(pred_np) == 0:
        metrics = {"samples": 0, "mse": float("nan"), "mae": float("nan"), "plcc": float("nan"), "srcc": float("nan")}
    else:
        metrics = {
            "samples": int(len(pred_np)),
            "mse": float(np.mean((target_np - pred_np) ** 2)),
            "mae": float(np.mean(np.abs(target_np - pred_np))),
            "plcc": _safe_corr(pearsonr, target_np, pred_np),
            "srcc": _safe_corr(spearmanr, target_np, pred_np),
        }

    prediction_df = pd.DataFrame(
        {
            "asset_id": asset_ids,
            "user_id": user_ids,
            "split": splits,
            "target": targets,
            "prediction": predictions,
        }
    )
    return metrics, prediction_df


def flatten_config(config: dict, parent: str = "") -> dict[str, str | int | float | bool | None]:
    flattened: dict[str, str | int | float | bool | None] = {}
    for key, value in config.items():
        full_key = f"{parent}.{key}" if parent else str(key)
        if isinstance(value, dict):
            flattened.update(flatten_config(value, full_key))
        elif isinstance(value, (str, int, float, bool)) or value is None:
            flattened[full_key] = value
        else:
            flattened[full_key] = json.dumps(value)
    return flattened
