"""
plot_saver.py — central matplotlib configuration + save logic.

Every EDA plot goes through PlotSaver. The style matches the ChurnGuard
dashboard (JetBrains Mono + GitHub-dark palette) so plots look native
when rendered inside the dev dashboard.

PlotSaver.save() returns the *relative* path (relative to reports/) so
JSON artifacts stay portable — you can copy reports/ to any machine
and the paths still resolve.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt

from src.ml.eda.utils.signal_logger import SignalLogger


class PlotSaver(SignalLogger):
    """Holds the matplotlib style + save logic for all EDA plots."""

    # Mirrors the dashboard's CSS custom properties so plots look native in-UI.
    COLORS: Dict[str, str] = {
        "bg":       "#0d1117",
        "panel":    "#111820",
        "text":     "#e6edf3",
        "text_dim": "#8b949e",
        "blue":     "#58a6ff",
        "green":    "#3fb950",   # retained / low risk
        "red":      "#f85149",   # churn / high risk
        "purple":   "#bc8cff",
        "orange":   "#e3b341",
        "cyan":     "#79c0ff",
    }

    DPI: int = 140
    FIGSIZE_DEFAULT = (8, 4.5)

    def __init__(
        self,
        reports_root: str | Path,
        figures_subdir: str = "figures",
    ) -> None:
        super().__init__()
        self.reports_root = Path(reports_root)
        self.figures_dir = self.reports_root / figures_subdir
        self.figures_dir.mkdir(parents=True, exist_ok=True)
        self._apply_style()

    @property
    def churn_palette(self) -> List[str]:
        """Colours for churn = 0 (retained) and churn = 1 (churned), in that order."""
        return [self.COLORS["green"], self.COLORS["red"]]

    def _apply_style(self) -> None:
        """Apply dark-theme rcParams once at construction time."""
        plt.rcParams.update({
            "figure.facecolor":  self.COLORS["bg"],
            "axes.facecolor":    self.COLORS["panel"],
            "axes.edgecolor":    self.COLORS["text_dim"],
            "axes.labelcolor":   self.COLORS["text"],
            "axes.titlecolor":   self.COLORS["text"],
            "xtick.color":       self.COLORS["text_dim"],
            "ytick.color":       self.COLORS["text_dim"],
            "text.color":        self.COLORS["text"],
            "axes.grid":         True,
            "grid.color":        self.COLORS["text_dim"],
            "grid.alpha":        0.08,
            "grid.linestyle":    "-",
            "grid.linewidth":    0.5,
            "font.family":       "monospace",
            "font.monospace":    ["JetBrains Mono", "DejaVu Sans Mono", "monospace"],
            "font.size":         9,
            "axes.titlesize":    11,
            "axes.labelsize":    10,
            "legend.fontsize":   9,
            "figure.titlesize":  12,
            "axes.spines.top":   False,
            "axes.spines.right": False,
            "axes.titlepad":     12,
            "figure.dpi":        self.DPI,
            "savefig.dpi":       self.DPI,
            "savefig.facecolor": self.COLORS["bg"],
            "savefig.edgecolor": self.COLORS["bg"],
            "savefig.bbox":      "tight",
        })

    def save(self, fig, name: str) -> str:
        """Save figure as PNG, close it, return path relative to reports/.

        Args:
            fig:  a matplotlib Figure.
            name: filename stem (no extension, no path). Will be sanitised.

        Returns:
            Relative path string (e.g. 'figures/num__tenure__histogram.png')
            that downstream JSON artifacts can embed directly.
        """
        safe_name = name.replace("/", "_").replace(" ", "_")
        abs_path = self.figures_dir / f"{safe_name}.png"
        fig.savefig(abs_path)
        plt.close(fig)
        rel_path = abs_path.relative_to(self.reports_root).as_posix()
        self._signal("Saved plot", f"reports/{rel_path}")
        return rel_path
