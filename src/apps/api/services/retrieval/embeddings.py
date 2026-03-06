from __future__ import annotations

import logging
from functools import lru_cache
from typing import Iterable

from apps.api.core.config import get_settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    def __init__(self):
        self.settings = get_settings()
        self._openai_client = None
        self._local_model = None
        self.backend = self._resolve_backend()
        self.dimension = self._infer_dimension()

    def _resolve_backend(self) -> str:
        backend = self.settings.embedding_backend.lower()
        if backend == "openai":
            return "openai"
        if backend == "local":
            return "local"
        if self.settings.resolved_openai_embedding_api_key:
            return "openai"
        return "local"

    def _infer_dimension(self) -> int:
        if self.backend == "openai":
            # text-embedding-3-small default dimension.
            return 1536
        return 384

    def _get_openai_client(self):
        if self._openai_client is None:
            from openai import OpenAI

            self._openai_client = OpenAI(api_key=self.settings.resolved_openai_embedding_api_key)
        return self._openai_client

    def _get_local_model(self):
        if self._local_model is None:
            from sentence_transformers import SentenceTransformer

            self._local_model = SentenceTransformer(self.settings.local_embedding_model)
        return self._local_model

    def _embed_openai(self, texts: list[str]) -> list[list[float]]:
        client = self._get_openai_client()
        resp = client.embeddings.create(model=self.settings.openai_embedding_model, input=texts)
        return [item.embedding for item in resp.data]

    def _embed_local(self, texts: list[str]) -> list[list[float]]:
        model = self._get_local_model()
        vecs = model.encode(texts, normalize_embeddings=True)
        return [v.tolist() for v in vecs]

    def embed_texts(self, texts: Iterable[str]) -> list[list[float]]:
        text_list = [t if isinstance(t, str) else "" for t in texts]
        if not text_list:
            return []

        if self.backend == "openai":
            try:
                return self._embed_openai(text_list)
            except Exception as exc:
                logger.warning("OpenAI embedding failed. Falling back to local model: %s", exc)
                self.backend = "local"
                self.dimension = 384
                return self._embed_local(text_list)
        return self._embed_local(text_list)

    def embed_query(self, text: str) -> list[float]:
        return self.embed_texts([text])[0]


@lru_cache(maxsize=1)
def get_embedding_service() -> EmbeddingService:
    return EmbeddingService()
