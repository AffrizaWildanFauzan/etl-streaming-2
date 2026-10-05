"""Regression guard: jendela agregasi DAG harus dikunci ke jam bulat.

Airflow tidak dipasang di lingkungan test, jadi DAG dibaca sebagai teks.
"""
from pathlib import Path

DAG = (Path(__file__).resolve().parents[1] / "dags" / "traffic_hourly_mart.py").read_text(encoding="utf-8")


def test_window_is_anchored_to_whole_hours():
    assert "date_trunc('hour', TIMESTAMPTZ '{{ data_interval_end }}')" in DAG


def test_raw_data_interval_is_not_used_as_window_bound():
    for bound in ("observed_at >= '{{ data_interval_start }}'", "hour >= '{{ data_interval_start }}'"):
        assert bound not in DAG, f"jendela masih memakai data_interval mentah: {bound}"
