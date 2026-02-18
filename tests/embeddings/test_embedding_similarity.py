"""
Tests for benchmarking different embeddings models against each other.

In order to run theste tests you'll need to have first downloaded all of the
embeddings models and stored them in the sentence-transformers cache directory.
"""
from __future__ import annotations

import math
import time
from typing import NamedTuple, cast

import pytest

from dank.embeddings import EmbeddingModel

pytestmark = [
    pytest.mark.embeddings,
    pytest.mark.integration,
]


class _ModelSpec(NamedTuple):
    case_id: str
    model_name: str


# Different models to compare.
MODEL_SPECS = (
    _ModelSpec(
        case_id="all-MiniLM-L6-v2",
        model_name="sentence-transformers/all-MiniLM-L6-v2",
    ),
    _ModelSpec(
        case_id="paraphrase-MiniLM-L3-v2",
        model_name="sentence-transformers/paraphrase-MiniLM-L3-v2",
    ),
    _ModelSpec(
        case_id="multi-qa-MiniLM-L6-cos-v1",
        model_name="sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
    ),
    _ModelSpec(
        case_id="gte-small",
        model_name="thenlper/gte-small",
    ),
    _ModelSpec(
        case_id="bge-small-en-v1.5",
        model_name="BAAI/bge-small-en-v1.5",
    ),
)


class _SimilarityCase(NamedTuple):
    case_id: str
    anchor: str
    related: str
    unrelated: str
    min_margin: float


ANONYMIZED_SIMILARITY_CASES = (
    _SimilarityCase(
        case_id="sports-news",
        anchor=(
            "A football club will host a bigger opponent in a cup "
            "fifth-round match."
        ),
        related=(
            "A player says he enjoys a wing-back role for that same "
            "football club."
        ),
        unrelated=(
            "The unemployment rate climbed to the highest level in "
            "nearly five years."
        ),
        min_margin=0.04,
    ),
    _SimilarityCase(
        case_id="survival-game-release",
        anchor="A new survival horror game is now available on PC.",
        related="An indie survival horror RPG launched for players.",
        unrelated=(
            "The unemployment rate climbed to the highest level in "
            "nearly five years."
        ),
        min_margin=0.06,
    ),
    _SimilarityCase(
        case_id="game-updates",
        anchor=(
            "An action game update launches in February with extra "
            "content."
        ),
        related=(
            "A strategy RPG receives a free February update and new "
            "downloadable content."
        ),
        unrelated=(
            "The unemployment rate climbed to the highest level in "
            "nearly five years."
        ),
        min_margin=0.03,
    ),
)


class _BenchmarkResult(NamedTuple):
    case_id: str
    average_margin: float
    minimum_margin: float
    texts_per_second: float


BENCHMARK_TEXTS = (
    "A football club will host a bigger opponent in a cup fifth-round match.",
    "A player says he enjoys a wing-back role for that same football club.",
    "An action game update launches in February with extra content.",
    (
        "A strategy RPG receives a free February update and new "
        "downloadable content."
    ),
    "A new survival horror game is now available on PC.",
    "An indie survival horror RPG launched for players.",
    "The unemployment rate climbed to the highest level in nearly five years.",
    "A city council approved a housing budget increase this week.",
)


def _is_missing_model_error(message: str) -> bool:
    return any(
        part in message
        for part in (
            "network is disabled in tests",
            "couldn't connect",
            "cannot find",
            "can't load",
            "not found in local",
            "local_files_only",
        )
    )


@pytest.fixture(
    scope="module",
    params=MODEL_SPECS,
    ids=[spec.case_id for spec in MODEL_SPECS],
)
def model_under_test(
    request: pytest.FixtureRequest,
) -> tuple[_ModelSpec, EmbeddingModel]:
    spec = cast(_ModelSpec, request.param)
    model = EmbeddingModel(
        model_name=spec.model_name,
        local_files_only=True,
    )

    try:
        model.ensure_model_loaded()
    except (OSError, RuntimeError, ValueError) as error:
        message = str(error).lower()

        if _is_missing_model_error(message):
            pytest.skip(
                "Model is not cached locally. "
                "Run `uv run download-embedding-model --model "
                f"{spec.model_name}` first.",
            )

        raise

    return spec, model


def _compute_case_scores(
    model: EmbeddingModel,
    case: _SimilarityCase,
) -> tuple[float, float, float]:
    anchor_vector, related_vector, unrelated_vector = model.embed_texts(
        [
            case.anchor,
            case.related,
            case.unrelated,
        ],
    )
    related_similarity = _cosine_similarity(anchor_vector, related_vector)
    unrelated_similarity = _cosine_similarity(anchor_vector, unrelated_vector)
    margin = related_similarity - unrelated_similarity

    return related_similarity, unrelated_similarity, margin


def _cosine_similarity(
    left: tuple[float, ...],
    right: tuple[float, ...],
) -> float:
    numerator = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))

    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0

    return numerator / (left_norm * right_norm)


def test_embeddings_are_normalized(
    model_under_test: tuple[_ModelSpec, EmbeddingModel],
) -> None:
    spec, model = model_under_test
    vector = model.embed_texts(["Normalization check sentence"])[0]
    magnitude = math.sqrt(sum(value * value for value in vector))

    assert abs(magnitude - 1.0) <= 1e-4, (
        f"{spec.case_id} produced non-unit embedding norm={magnitude:.6f}"
    )


def test_identical_snippets_have_very_high_similarity(
    model_under_test: tuple[_ModelSpec, EmbeddingModel],
) -> None:
    spec, model = model_under_test
    text = "A city council approved a housing budget increase this week."
    left_vector, right_vector = model.embed_texts([text, text])
    similarity = _cosine_similarity(left_vector, right_vector)

    assert similarity > 0.99, (
        f"{spec.case_id} identical text similarity={similarity:.6f}"
    )


@pytest.mark.parametrize(
    "case",
    ANONYMIZED_SIMILARITY_CASES,
    ids=[case.case_id for case in ANONYMIZED_SIMILARITY_CASES],
)
def test_related_snippets_score_higher_than_unrelated(
    model_under_test: tuple[_ModelSpec, EmbeddingModel],
    case: _SimilarityCase,
) -> None:
    spec, model = model_under_test
    related_similarity, unrelated_similarity, margin = _compute_case_scores(
        model,
        case,
    )

    print(
        "model="
        f"{spec.case_id} "
        f"case={case.case_id} "
        f"related={related_similarity:.4f} "
        f"unrelated={unrelated_similarity:.4f} "
        f"margin={margin:.4f}",
    )

    assert related_similarity > unrelated_similarity
    assert margin >= case.min_margin


def test_similarity_margin_summary(
    model_under_test: tuple[_ModelSpec, EmbeddingModel],
) -> None:
    spec, model = model_under_test
    margins: list[float] = []

    for case in ANONYMIZED_SIMILARITY_CASES:
        _, _, margin = _compute_case_scores(model, case)
        margins.append(margin)

    average_margin = sum(margins) / len(margins)
    minimum_margin = min(margins)

    print(
        "summary "
        f"model={spec.case_id} "
        f"avg_margin={average_margin:.4f} "
        f"min_margin={minimum_margin:.4f}",
    )

    assert average_margin > 0.0


def _compute_margin_summary(model: EmbeddingModel) -> tuple[float, float]:
    margins: list[float] = []

    for case in ANONYMIZED_SIMILARITY_CASES:
        _, _, margin = _compute_case_scores(model, case)
        margins.append(margin)

    return sum(margins) / len(margins), min(margins)


def _measure_texts_per_second(model: EmbeddingModel) -> float:
    benchmark_inputs = [str(value) for value in BENCHMARK_TEXTS] * 8
    model.embed_texts(benchmark_inputs[:8])
    timings: list[float] = []

    for _ in range(3):
        started = time.perf_counter()
        vectors = model.embed_texts(benchmark_inputs)
        elapsed = time.perf_counter() - started

        assert len(vectors) == len(benchmark_inputs)
        timings.append(elapsed)

    median_elapsed = sorted(timings)[1]

    return len(benchmark_inputs) / median_elapsed


def test_speed_ranked_against_accuracy() -> None:
    results: list[_BenchmarkResult] = []

    for spec in MODEL_SPECS:
        model = EmbeddingModel(
            model_name=spec.model_name,
            local_files_only=True,
        )

        try:
            model.ensure_model_loaded()
        except (OSError, RuntimeError, ValueError) as error:
            message = str(error).lower()

            if _is_missing_model_error(message):
                pytest.skip(
                    "Model is not cached locally. "
                    "Run `uv run download-embedding-model --model "
                    f"{spec.model_name}` first.",
                )

            raise

        average_margin, minimum_margin = _compute_margin_summary(model)
        texts_per_second = _measure_texts_per_second(model)
        results.append(
            _BenchmarkResult(
                case_id=spec.case_id,
                average_margin=average_margin,
                minimum_margin=minimum_margin,
                texts_per_second=texts_per_second,
            ),
        )

    assert results

    by_accuracy = sorted(
        results,
        key=lambda result: result.average_margin,
        reverse=True,
    )
    by_speed = sorted(
        results,
        key=lambda result: result.texts_per_second,
        reverse=True,
    )
    accuracy_rank = {
        result.case_id: index
        for index, result in enumerate(by_accuracy, start=1)
    }
    speed_rank = {
        result.case_id: index
        for index, result in enumerate(by_speed, start=1)
    }
    by_balance = sorted(
        results,
        key=lambda result: (
            accuracy_rank[result.case_id] + speed_rank[result.case_id],
            -result.average_margin,
            -result.texts_per_second,
        ),
    )

    print("speed-vs-accuracy ranking (lower combined rank is better):")

    for index, result in enumerate(by_balance, start=1):
        print(
            "rank="
            f"{index} "
            f"model={result.case_id} "
            f"combined_rank="
            f"{accuracy_rank[result.case_id] + speed_rank[result.case_id]} "
            f"accuracy_rank={accuracy_rank[result.case_id]} "
            f"speed_rank={speed_rank[result.case_id]} "
            f"avg_margin={result.average_margin:.4f} "
            f"min_margin={result.minimum_margin:.4f} "
            f"texts_per_sec={result.texts_per_second:.1f}",
        )
