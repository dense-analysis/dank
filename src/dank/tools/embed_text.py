from __future__ import annotations

import argparse

from dank.embedding_vectors import Vector
from dank.embeddings import get_embedding_model


def embed_text(text: str) -> Vector:
    embedding_model = get_embedding_model()

    return embedding_model.embed_texts([text])[0]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dank.tools.embed_text")
    parser.add_argument(
        "text",
        help="Text string to convert into an embedding",
    )
    args = parser.parse_args(argv)
    embedding = embed_text(args.text)
    print(list(embedding))


if __name__ == "__main__":
    main()
