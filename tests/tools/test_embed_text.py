import json
from typing import Any, cast

from dank.tools import embed_text


class _DummyEmbedder:
    def __init__(self) -> None:
        self.items: list[str] = []

    def embed_texts(self, items: list[str]) -> list[list[float]]:
        self.items.extend(items)

        return [[0.5, -0.25, 0.0] for _ in items]


def test_embed_text_returns_model_result(monkeypatch: Any) -> None:
    model = _DummyEmbedder()
    monkeypatch.setattr(
        embed_text,
        "get_embedding_model",
        lambda: cast(Any, model),
    )

    result = embed_text.embed_text("hello embeddings")

    assert result == [0.5, -0.25, 0.0]
    assert model.items == ["hello embeddings"]


def test_main_prints_embedding_as_json(monkeypatch: Any, capsys: Any) -> None:
    model = _DummyEmbedder()
    monkeypatch.setattr(
        embed_text,
        "get_embedding_model",
        lambda: cast(Any, model),
    )

    embed_text.main(["hello world"])

    output = capsys.readouterr().out.strip()

    assert json.loads(output) == [0.5, -0.25, 0.0]
    assert model.items == ["hello world"]
