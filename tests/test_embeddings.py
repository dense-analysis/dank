from typing import Any

from dank.embedding_vectors import EMPTY_STRING_VECTOR
from dank.embeddings import EmbeddingModel


class _DummySentenceTransformer:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def encode(
        self,
        items: list[str],
        *,
        convert_to_numpy: bool,
        normalize_embeddings: bool,
        show_progress_bar: bool,
    ) -> list[tuple[float, ...]]:
        assert convert_to_numpy
        assert normalize_embeddings
        assert not show_progress_bar
        self.calls.append(items)

        return [(float(index),) for index in range(1, len(items) + 1)]


class _TestEmbeddingModel(EmbeddingModel):
    def __init__(self, model: Any | None = None) -> None:
        super().__init__()
        self.model = model
        self.get_model_calls = 0

    def _get_model(self) -> Any:
        self.get_model_calls += 1

        if self.model is None:
            raise AssertionError("_get_model should not be called")

        return self.model


def test_embed_texts_strips_and_preserves_order() -> None:
    dummy = _DummySentenceTransformer()
    embedder = _TestEmbeddingModel(dummy)

    result = embedder.embed_texts(["  first  ", "", "\t second\n"])

    assert dummy.calls == [["first", "second"]]
    assert result == [(1.0,), EMPTY_STRING_VECTOR, (2.0,)]


def test_embed_texts_all_empty_uses_precomputed_vector_without_model() -> None:
    embedder = _TestEmbeddingModel()

    first = embedder.embed_texts([" ", "\n"])
    second = embedder.embed_texts(["", "\t"])

    assert first == [EMPTY_STRING_VECTOR, EMPTY_STRING_VECTOR]
    assert second == [EMPTY_STRING_VECTOR, EMPTY_STRING_VECTOR]
    assert embedder.get_model_calls == 0


def test_embed_text_empty_returns_precomputed_vector_without_model() -> None:
    embedder = _TestEmbeddingModel()

    first = embedder.embed_texts(["   "])[0]
    second = embedder.embed_texts(["\t"])[0]

    assert first == EMPTY_STRING_VECTOR
    assert second == EMPTY_STRING_VECTOR
    assert first is second
    assert embedder.get_model_calls == 0
