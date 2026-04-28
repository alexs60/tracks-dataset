from __future__ import annotations

import json
from dataclasses import dataclass


BINARY_CLASSIFIERS = {
    "timbre": "dark",
    "tonal_atonal": "tonal",
    "danceability": "danceable",
    "voice_instrumental": "voice",
    "gender": "female",
    "mood_acoustic": "acoustic",
    "mood_electronic": "electronic",
    "mood_relaxed": "relaxed",
    "mood_sad": "sad",
    "mood_party": "party",
    "mood_happy": "happy",
    "mood_aggressive": "aggressive",
}

CATEGORICAL_CLASSIFIERS = {
    "moods_mirex",
    "genre_electronic",
    "genre_tzanetakis",
    "genre_dortmund",
    "genre_rosamerica",
    "rhythm_ismir04",
}


@dataclass
class ParsedEssentia:
    analysis_row: dict[str, object]
    binary_rows: list[dict[str, object]]
    categorical_rows: list[dict[str, object]]
    categorical_prob_rows: list[dict[str, object]]


def _normalize_label(label: str) -> str:
    return label.strip().lower().replace("-", "_").replace(" ", "_")


def _winner(probabilities: dict[str, float]) -> tuple[str, float]:
    winner_label, winner_prob = max(probabilities.items(), key=lambda item: item[1])
    return winner_label, float(winner_prob)


def _as_dict(node: object) -> dict[str, object]:
    return node if isinstance(node, dict) else {}


def parse_essentia_json(track_id: str, payload: dict[str, object], analyzed_at: str) -> ParsedEssentia:
    metadata = _as_dict(payload.get("metadata"))
    metadata_version = _as_dict(metadata.get("version"))
    versions = _as_dict(payload.get("version"))
    extractor_version = (
        versions.get("essentia")
        or metadata_version.get("essentia")
        or metadata_version.get("extractor")
        or payload.get("version")
        or "unknown"
    )
    models_version = metadata.get("models")
    if not models_version:
        models_version = metadata_version.get("extractor")
    if not models_version:
        models_version = "unknown"

    rhythm = _as_dict(payload.get("rhythm"))
    tonal = _as_dict(payload.get("tonal"))
    lowlevel = _as_dict(payload.get("lowlevel"))
    highlevel = _as_dict(payload.get("highlevel"))
    loudness_ebu128 = _as_dict(lowlevel.get("loudness_ebu128"))
    audio_properties = _as_dict(metadata.get("audio_properties"))
    key_edma = _as_dict(tonal.get("key_edma"))
    key_krumhansl = _as_dict(tonal.get("key_krumhansl"))
    key_temperley = _as_dict(tonal.get("key_temperley"))

    analysis_row = {
        "track_id": track_id,
        "analyzed_at": analyzed_at,
        "extractor_version": str(extractor_version),
        "models_version": str(models_version),
        "status": "ok",
        "error": None,
        "raw_json": json.dumps(payload, sort_keys=True),
        "bpm": rhythm.get("bpm"),
        "bpm_confidence": rhythm.get("bpm_confidence"),
        "danceability_raw": rhythm.get("danceability"),
        "loudness_ebu128": loudness_ebu128.get("integrated"),
        "average_loudness": lowlevel.get("average_loudness"),
        "dynamic_complexity": lowlevel.get("dynamic_complexity"),
        "key_key": tonal.get("key_key") or key_edma.get("key") or key_krumhansl.get("key") or key_temperley.get("key"),
        "key_scale": tonal.get("key_scale") or key_edma.get("scale") or key_krumhansl.get("scale") or key_temperley.get("scale"),
        "key_strength": tonal.get("key_strength") or key_edma.get("strength") or key_krumhansl.get("strength") or key_temperley.get("strength"),
        "chords_changes_rate": tonal.get("chords_changes_rate"),
        "tuning_frequency": tonal.get("tuning_frequency"),
        "onset_rate": rhythm.get("onset_rate"),
        "duration_sec": audio_properties.get("length"),
    }

    binary_rows: list[dict[str, object]] = []
    categorical_rows: list[dict[str, object]] = []
    categorical_prob_rows: list[dict[str, object]] = []

    for classifier, positive_label in BINARY_CLASSIFIERS.items():
        node = highlevel.get(classifier)
        if not isinstance(node, dict):
            continue
        all_probs = node.get("all")
        if not isinstance(all_probs, dict) or not all_probs:
            continue
        winner_label, winner_prob = _winner(all_probs)
        normalized_probs = {_normalize_label(key): float(value) for key, value in all_probs.items()}
        prob_positive = normalized_probs.get(_normalize_label(positive_label))
        if prob_positive is None:
            raise ValueError(
                f"Missing positive-class mapping for {classifier}: expected {positive_label}, found {sorted(all_probs)}"
            )
        binary_rows.append({
            "track_id": track_id,
            "classifier": classifier,
            "value": winner_label,
            "probability": winner_prob,
            "prob_positive": prob_positive,
        })

    for classifier in CATEGORICAL_CLASSIFIERS:
        node = highlevel.get(classifier)
        if classifier == "rhythm_ismir04" and not isinstance(node, dict):
            node = highlevel.get("ismir04_rhythm")
        if not isinstance(node, dict):
            continue
        all_probs = node.get("all")
        if not isinstance(all_probs, dict) or not all_probs:
            continue
        winner_label, winner_prob = _winner(all_probs)
        categorical_rows.append({
            "track_id": track_id,
            "classifier": classifier,
            "value": winner_label,
            "probability": winner_prob,
        })
        for class_label, probability in all_probs.items():
            categorical_prob_rows.append({
                "track_id": track_id,
                "classifier": classifier,
                "class_label": class_label,
                "probability": float(probability),
            })

    return ParsedEssentia(
        analysis_row=analysis_row,
        binary_rows=binary_rows,
        categorical_rows=categorical_rows,
        categorical_prob_rows=categorical_prob_rows,
    )