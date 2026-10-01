"""Analyze randomized experiment events with visitor-level inference."""

import json
import sys
from pathlib import Path

import duckdb
from scipy.stats import ttest_ind
from statsmodels.stats.proportion import proportion_confint, proportions_ztest

from recsys.config import settings


def analyze(event_glob: str, output: Path) -> dict:
    con = duckdb.connect()
    visitors = con.execute(
        """WITH events AS (SELECT * EXCLUDE (event_row) FROM (
          SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY timestamp_ms) event_row
          FROM read_json_auto(?, union_by_name=true)) WHERE event_row = 1),
        assigned AS (SELECT experiment_id, visitor_id, variant,
                     min(timestamp_ms) AS assigned_at
                     FROM events WHERE event_type='assignment'
                     GROUP BY experiment_id, visitor_id, variant),
        outcomes AS (SELECT experiment_id, visitor_id,
          max(CASE WHEN event_type='click' THEN 1 ELSE 0 END) AS clicked,
          max(CASE WHEN event_type='transaction' THEN 1 ELSE 0 END) AS purchased,
          sum(CASE WHEN event_type='transaction' THEN coalesce(purchase_amount, 0)
              ELSE 0 END) AS revenue
          FROM events GROUP BY experiment_id, visitor_id)
        SELECT a.experiment_id, a.variant, a.visitor_id,
               coalesce(o.clicked,0) clicked, coalesce(o.purchased,0) purchased,
               coalesce(o.revenue,0) revenue
        FROM assigned a LEFT JOIN outcomes o USING (experiment_id, visitor_id)""",
        [event_glob],
    ).df()
    con.close()
    if visitors.empty or set(visitors["variant"].unique()) != {"control", "treatment"}:
        raise ValueError("Need assignment and outcome events from both experiment variants")
    if visitors.experiment_id.nunique() != 1:
        raise ValueError(
            "Analyze one experiment_id at a time; the input contains multiple experiments"
        )
    metrics = {}
    control = visitors[visitors.variant == "control"]
    treatment = visitors[visitors.variant == "treatment"]
    for name, column in (("click_rate", "clicked"), ("purchase_rate", "purchased")):
        counts = [int(control[column].sum()), int(treatment[column].sum())]
        sizes = [len(control), len(treatment)]
        z_stat, p_value = proportions_ztest(counts, sizes)
        control_ci = proportion_confint(counts[0], sizes[0], method="wilson")
        treatment_ci = proportion_confint(counts[1], sizes[1], method="wilson")
        metrics[name] = {
            "control": counts[0] / sizes[0],
            "treatment": counts[1] / sizes[1],
            "absolute_lift": counts[1] / sizes[1] - counts[0] / sizes[0],
            "control_ci_95": [float(control_ci[0]), float(control_ci[1])],
            "treatment_ci_95": [float(treatment_ci[0]), float(treatment_ci[1])],
            "p_value": float(p_value),
            "z_statistic": float(z_stat),
        }
    revenue_test = ttest_ind(treatment.revenue, control.revenue, equal_var=False)
    metrics["revenue_per_visitor"] = {
        "control": float(control.revenue.mean()),
        "treatment": float(treatment.revenue.mean()),
        "absolute_lift": float(treatment.revenue.mean() - control.revenue.mean()),
        "p_value": float(revenue_test.pvalue),
        "lift_ci_95": [
            float(revenue_test.confidence_interval().low),
            float(revenue_test.confidence_interval().high),
        ],
    }
    report = {
        "experiment_id": str(visitors.experiment_id.iloc[0]),
        "control_visitors": len(control),
        "treatment_visitors": len(treatment),
        "metrics": metrics,
        "warning": "Observational Retailrocket history is not a randomized experiment. "
        "Use these tests only for events collected after stable random assignment.",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(settings.event_log_dir / "events-*.jsonl")
    print(json.dumps(analyze(pattern, settings.artifact_dir / "experiment_report.json"), indent=2))
