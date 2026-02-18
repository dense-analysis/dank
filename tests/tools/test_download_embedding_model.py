from typing import Any, cast

from dank.tools import download_embedding_model


class _DummyEmbeddingModel:
    def __init__(
        self,
        model_name: str,
        device: str,
        *,
        local_files_only: bool = False,
    ) -> None:
        self.model_name = model_name
        self.device = device
        self.local_files_only = local_files_only
        self.ensure_calls = 0

    def ensure_model_loaded(self) -> None:
        self.ensure_calls += 1


def test_download_embedding_model_loads_expected_default_model(
    monkeypatch: Any,
) -> None:
    created_models: list[_DummyEmbeddingModel] = []

    def factory(
        model_name: str,
        device: str,
        *,
        local_files_only: bool = False,
    ) -> _DummyEmbeddingModel:
        model = _DummyEmbeddingModel(
            model_name,
            device,
            local_files_only=local_files_only,
        )
        created_models.append(model)

        return model

    monkeypatch.setattr(
        download_embedding_model,
        "EmbeddingModel",
        cast(Any, factory),
    )

    download_embedding_model.download_embedding_model()

    assert len(created_models) == 1
    assert created_models[0].model_name == download_embedding_model.MODEL_NAME
    assert created_models[0].device == "cpu"
    assert created_models[0].ensure_calls == 1


def test_main_parses_arguments_and_prints_status(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_download_embedding_model(
        *,
        model_name: str,
        device: str,
    ) -> None:
        calls.append((model_name, device))

    monkeypatch.setattr(
        download_embedding_model,
        "download_embedding_model",
        cast(Any, fake_download_embedding_model),
    )

    download_embedding_model.main(
        [
            "--model",
            "sentence-transformers/paraphrase-MiniLM-L3-v2",
            "--device",
            "cpu",
        ],
    )

    output = capsys.readouterr().out

    assert calls == [
        (
            "sentence-transformers/paraphrase-MiniLM-L3-v2",
            "cpu",
        ),
    ]
    assert "Downloaded embedding model" in output
