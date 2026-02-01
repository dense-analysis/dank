from __future__ import annotations

from functools import lru_cache

from sentence_transformers import SentenceTransformer

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


class EmbeddingModel:
    def __init__(
        self,
        model_name: str = MODEL_NAME,
        device: str = "cpu",
    ) -> None:
        self._model = SentenceTransformer(model_name, device=device)
        self._model.eval()

    def embed_text(self, text: str) -> list[float]:
        embeddings = self.embed_texts([text])

        return embeddings[0] if embeddings else []

    def embed_texts(self, items: list[str]) -> list[list[float]]:
        if not items:
            return []

        embeddings = self._model.encode(  # type: ignore
            items,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        return [[float(value) for value in row] for row in embeddings]


@lru_cache(maxsize=1)
def get_embedding_model() -> EmbeddingModel:
    return EmbeddingModel()
