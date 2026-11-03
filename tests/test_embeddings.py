import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any

import pytest

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


class _LoadWatcher:
    """A stand-in model class that notices overlapping and repeated loads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.loading = 0
        self.overlapped = False
        self.loads = 0

    def transformer(self, *_args: Any, **_kwargs: Any) -> Any:
        with self.lock:
            self.loading += 1
            self.loads += 1
            self.overlapped = self.overlapped or self.loading > 1

        # Long enough for the other threads to arrive while this one loads.
        time.sleep(0.05)

        with self.lock:
            self.loading -= 1

        return SimpleNamespace(eval=lambda: None)


@pytest.fixture
def watcher(monkeypatch: pytest.MonkeyPatch) -> _LoadWatcher:
    found = _LoadWatcher()
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        SimpleNamespace(SentenceTransformer=found.transformer),
    )

    return found


def _load_together(*models: EmbeddingModel) -> list[Any]:
    start = threading.Barrier(len(models))

    def load(model: EmbeddingModel) -> Any:
        start.wait()

        return model._get_model()  # pyright: ignore[reportPrivateUsage]

    with ThreadPoolExecutor(len(models)) as pool:
        return list(pool.map(load, models))


def test_concurrent_first_uses_load_the_model_once(
    watcher: _LoadWatcher,
) -> None:
    model = EmbeddingModel()

    found = _load_together(model, model, model, model)

    assert watcher.loads == 1
    assert all(item is found[0] for item in found)


def test_separate_instances_never_load_at_the_same_time(
    watcher: _LoadWatcher,
) -> None:
    # Loading changes process-wide state in transformers, which is what made
    # concurrent loads fail with "Cannot copy out of meta tensor".
    _load_together(EmbeddingModel(), EmbeddingModel(), EmbeddingModel())

    assert watcher.loads == 3
    assert not watcher.overlapped
