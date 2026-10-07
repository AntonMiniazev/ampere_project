"""Keyed Polars frame comparisons for local data checks."""

from __future__ import annotations

import polars as pl


def compare_frames(
    left: pl.DataFrame,
    right: pl.DataFrame,
    keys: tuple[str, ...],
    fields: tuple[str, ...] = (),
    *,
    left_label: str = "left",
    right_label: str = "right",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return counts and differing keys/values for uniquely keyed frames."""
    if not keys or left_label == right_label:
        raise ValueError("Provide keys and distinct side labels")
    for key in keys:
        if left.schema[key] == pl.Null:
            left = left.with_columns(pl.col(key).cast(right.schema[key]))
        else:
            right = right.with_columns(pl.col(key).cast(left.schema[key]))
    for label, frame in ((left_label, left), (right_label, right)):
        duplicates = frame.group_by(list(keys)).len().filter(pl.col("len") > 1)
        if not duplicates.is_empty():
            raise ValueError(f"{label} has duplicate comparison keys: {duplicates.head(5)}")
    missing = left.join(
        right.select(list(keys)), on=list(keys), how="anti", nulls_equal=True
    ).with_columns(pl.lit(f"missing_in_{right_label}").alias("issue"))
    extra = right.join(
        left.select(list(keys)), on=list(keys), how="anti", nulls_equal=True
    ).with_columns(pl.lit(f"{right_label}_only").alias("issue"))

    def key_issue(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.select([
            *keys,
            "issue",
            pl.lit(None, dtype=pl.String).alias("field"),
            pl.lit(None, dtype=pl.String).alias(f"{left_label}_value"),
            pl.lit(None, dtype=pl.String).alias(f"{right_label}_value"),
        ])

    details = [key_issue(missing), key_issue(extra)]
    changed_count = 0
    if fields:
        paired = left.join(
            right, on=list(keys), how="inner", suffix="_right", nulls_equal=True
        )
        changed = paired.filter(pl.any_horizontal([
            ~pl.col(field).eq_missing(pl.col(f"{field}_right")) for field in fields
        ]))
        changed_count = changed.height
        for field in fields:
            different = changed.filter(~pl.col(field).eq_missing(pl.col(f"{field}_right")))
            details.append(different.select([
                *keys,
                pl.lit("value_mismatch").alias("issue"),
                pl.lit(field).alias("field"),
                pl.col(field).cast(pl.String).alias(f"{left_label}_value"),
                pl.col(f"{field}_right").cast(pl.String).alias(f"{right_label}_value"),
            ]))
    summary = pl.DataFrame({
        f"{left_label}_rows": [left.height],
        f"{right_label}_rows": [right.height],
        f"missing_in_{right_label}": [missing.height],
        f"{right_label}_only": [extra.height],
        "value_mismatch": [changed_count],
    })
    return summary, pl.concat(details, how="vertical")
