"""
Runs ONNX Runtime in-process instead of calling Triton over HTTP.
"""
import logging
import numpy as np
import onnxruntime as rt
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

LOW_CONF_THRESHOLD = 0.15
DIVERGENCE_THRESHOLD = 0.40


class Scorer:
    def __init__(self, global_model_path: str, personalized_model_path: Optional[str] = None):
        logger.info(f"[scorer] Loading global model from {global_model_path}")
        self.global_sess = rt.InferenceSession(
            global_model_path,
            providers=["CPUExecutionProvider"],
        )
        self.personalized_sess: Optional[rt.InferenceSession] = None
        if personalized_model_path:
            try:
                logger.info(f"[scorer] Loading personalized model from {personalized_model_path}")
                self.personalized_sess = rt.InferenceSession(
                    personalized_model_path,
                    providers=["CPUExecutionProvider"],
                )
            except Exception as e:
                logger.warning(f"[scorer] Failed to load personalized model: {e} — cold-start only")

        self.model_version: Optional[str] = None  # set by model_loader after download

    def _run_global(self, clip_emb: np.ndarray) -> float:
        """global_mlp: input[768] → output[1]"""
        inp = clip_emb.reshape(1, 768).astype(np.float32)
        result = self.global_sess.run(["output"], {"input": inp})
        return float(result[0][0][0])

    def _run_personalized(self, clip_emb: np.ndarray, user_emb: np.ndarray) -> float:
        """personalized_mlp: image_embedding[768] + user_embedding[64] → output[1]"""
        clip_inp = clip_emb.reshape(1, 768).astype(np.float32)
        user_inp = user_emb.reshape(1, 64).astype(np.float32)
        result = self.personalized_sess.run(
            ["output"],
            {"image_embedding": clip_inp, "user_embedding": user_inp},
        )
        return float(result[0][0][0])

    def score(
        self,
        clip_emb: np.ndarray,
        user_emb: Optional[np.ndarray],
        alpha: float,
        is_cold_start: bool,
    ) -> Tuple[float, float, Optional[float], float, bool]:
        """
        Returns (final_score, global_score, personalized_score, effective_alpha, low_confidence).
        """
        g_score = self._run_global(clip_emb)

        p_score: Optional[float] = None
        effective_alpha = alpha

        if (
            not is_cold_start
            and user_emb is not None
            and alpha > 0
            and self.personalized_sess is not None
        ):
            try:
                p_score = self._run_personalized(clip_emb, user_emb)
            except Exception as e:
                logger.warning(f"[scorer] Personalized inference failed, falling back to global: {e}")

        if p_score is not None:
            final_score = (1 - effective_alpha) * g_score + effective_alpha * p_score
            divergence = abs(g_score - p_score)
        else:
            final_score = g_score
            effective_alpha = 0.0
            divergence = 0.0

        low_confidence = (
            final_score < LOW_CONF_THRESHOLD or divergence > DIVERGENCE_THRESHOLD
        )

        return final_score, g_score, p_score, effective_alpha, low_confidence
