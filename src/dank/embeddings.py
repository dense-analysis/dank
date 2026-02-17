from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, cast

from dank.embedding_vectors import PRECOMPUTED_TEXT_VECTORS, Vector

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

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(
                self.model_name,
                device=self.device,
            )
            self._model.eval()

        return self._model

    def embed_texts(self, items: list[str]) -> list[Vector]:
        """
        Compute embeddings for a list of strings, returning the list of
        vectors for those strings in that order.
        """
        # Strip whitespace in items first.
        items = [item.strip() for item in items]
        # Fill out vectors list with precomputed values.
        vectors = [PRECOMPUTED_TEXT_VECTORS.get(item) for item in items]

        # If all vectors can be precomputed, then return them right away.
        # This means we can avoid loading the embeddings model.
        if all(x is not None for x in vectors):
            return cast(list[Vector], vectors)

        missing_indexes_by_text: dict[str, list[int]] = {}

        for index, item in enumerate(items):
            if vectors[index] is None:
                missing_indexes_by_text.setdefault(item, []).append(index)

        # Dynamically load the model when we need it.
        model = self._get_model()
        missing_items = list(missing_indexes_by_text)

        # Encode each unique value we need to once.
        tensors = model.encode(  # type: ignore
            missing_items,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        for item, tensor in zip(missing_items, tensors, strict=True):
            vector = tuple(float(x) for x in tensor)

            for index in missing_indexes_by_text[item]:
                vectors[index] = vector

        return cast(list[Vector], vectors)


@lru_cache(maxsize=1)
def get_embedding_model() -> EmbeddingModel:
    return EmbeddingModel()
