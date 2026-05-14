"""
manifest_writer.py — writes the figures manifest consumed by the dev dashboard.

Every figure produced during EDA is registered with ManifestWriter. At the end
of the run, write() dumps reports/figures/manifest.json which the dashboard
uses to render the image gallery without hardcoded filenames. This means Marco
can rename figures, add new ones, or delete some — the dashboard just reads
the manifest and adapts.

Schema of each entry:
    {
        "path":      "figures/num__tenure__histogram.png",   (relative to reports/)
        "caption":   "Histogram of tenure. Orange = mean…",
        "feature":   "tenure_months",
        "plot_type": "histogram",
        "context":   "numeric" | "categorical" | "global"
    }
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from src.ml.eda.utils.signal_logger import SignalLogger


class ManifestWriter(SignalLogger):
    """Accumulates figure metadata, writes manifest.json at the end."""

    def __init__(self) -> None:
        super().__init__()
        self._entries: List[Dict] = []

    def add(
        self,
        path: str,
        caption: str,
        feature: Optional[str] = None,
        plot_type: Optional[str] = None,
        context: str = "feature",
    ) -> None:
        """Register a figure. Called by analyzers every time they save a plot."""
        self._entries.append({
            "path":      path,
            "caption":   caption,
            "feature":   feature,
            "plot_type": plot_type,
            "context":   context,
        })

    @property
    def count(self) -> int:
        return len(self._entries)

    def write(self, output_path: str | Path) -> None:
        """Dump the manifest as JSON."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(self._entries),
            "entries": self._entries,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        self._signal(
            "Writing to",
            str(output_path),
            f"{len(self._entries)} figures registered",
        )
