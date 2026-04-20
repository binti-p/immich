"""
Scorer: runs inference via in-process ONNX Runtime (local dev) or Triton (k8s).
Controlled by USE_TRITON env var (default: false).
"""
import logging
import os
import numpy as np
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

USE_TRITON = os.environ.get("USE_TRITON", "false").lower() in ("true", "1", "yes")
LOW_CONF_THRESHOLD = 0.15
DIVERGENCE_THRESHOLD = 0.40


class Scorer:
    def __init__(self, global_model_path: str, personalized_model_path: Optional[str] = None):
        self.use_triton = USE_TRITON
        self.personalized_available = False
        self.model_version: Optional[str] = None

        if self.use_triton:
            import triton_client
            self._triton = triton_client
            self.personalized_available = True  # Triton always has both models loaded
            logger.info("[scorer] Using Triton for inference")
        else:
            import onnxruntime as rt
            logger.info(f"[scorer] Loading global model from {global_model_path}")
            self.global_sess = rt.InferenceSession(
                global_model_path,
                providers=["CPUExecutionProvider"],
            )
            self.personalized_sess = None
            if personalized_model_path:
                try:
                    logger.info(f"[scorer] Loading personalized model from {personalized_model_path}")
                    self.personalized_sess = rt.InferenceSession(
                        personalized_model_path,
                        providers=["CPUExecutionProvider"],
                    )
                    self.personalized_available = True
                except Exception as e:
                    logger.warning(f"[scorer] Failed to load personalized model: {e} — cold-start only")

    def _run_global(self, clip_emb: np.ndarray) -> float:
        if self.use_triton:
            return self._triton.infer_global(clip_emb)
        inp = clip_emb.reshape(1, 768).astype(np.float32)
        result = self.global_sess.run(["output"], {"input": inp})
        return float(result[0][0][0])

    def _run_personalized(self, clip_emb: np.ndarray, user_emb: np.ndarray) -> float:
        if self.use_triton:
            return self._triton.infer_personalized(clip_emb, user_emb)
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
            and self.personalized_available
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
