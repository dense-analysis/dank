from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# Avoid loading sentence transfomers until needed.
if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


class EmbeddingModel:
    def __init__(
        self,
        model_name: str = MODEL_NAME,
        device: str = "cpu",
    ) -> None:
        # We'll defer loading the model until needed.
        self._model: SentenceTransformer | None = None
        self.model_name = model_name
        self.device = device

    def embed_texts(self, items: list[str]) -> list[list[float]]:
        """
        Compute embeddings for a list of strings, returning the list of
        vectors for those strings in that order.
        """
        if not items:
            return []

        # Dynamically load the model when we need it.
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(
                self.model_name,
                device=self.device,
            )
            self._model.eval()

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
