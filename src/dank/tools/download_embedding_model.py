from __future__ import annotations

import argparse

from dank.embeddings import MODEL_NAME, EmbeddingModel


def download_embedding_model(
    *,
    model_name: str = MODEL_NAME,
    device: str = "cpu",
) -> None:
    embedding_model = EmbeddingModel(
        model_name=model_name,
        device=device,
    )
    embedding_model.ensure_model_loaded()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="dank.tools.download_embedding_model",
    )
    parser.add_argument(
        "--model",
        default=MODEL_NAME,
        help="Sentence Transformers model ID to download",
    )
    parser.add_argument(
        "--device",
        default="cpu",
        help="Device to initialize the model with",
    )
    args = parser.parse_args(argv)
    download_embedding_model(
        model_name=args.model,
        device=args.device,
    )
    print(f"Downloaded embedding model: {args.model}")


if __name__ == "__main__":
    main()
