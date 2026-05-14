"""
signal_logger.py — base class providing Marco's 'reading from / writing to / done'
output signal convention. Every EDA component inherits from SignalLogger so every
external side-effect is logged in a uniform, grep-friendly format:

    [ClassName] Reading from    → Supabase.customers ⋈ customer_features
    [ClassName] Writing to      → src/ml/eda/reports/sanity.json   (status=pass)
    [ClassName] DONE            → 5,630 rows × 20 cols             (1.84s)
"""
from __future__ import annotations

import logging


class SignalLogger:
    """Mixin-style base providing self._signal() for uniform pipeline output."""

    def __init__(self) -> None:
        # One logger per concrete class module so filter configs stay flexible.
        self.logger = logging.getLogger(self.__class__.__module__)

    def _signal(self, action: str, target: str, detail: str = "") -> None:
        """Emit a standardised bracketed-prefix info line.

        Args:
            action:  short verb phrase, e.g. 'Reading from', 'Writing to', 'DONE'.
                     Padded to 14 chars for column alignment.
            target:  the external resource being acted upon.
            detail:  optional short suffix (elapsed time, row count, status flag).
        """
        tag = f"[{self.__class__.__name__}]"
        msg = f"{tag} {action:<14} → {target}"
        if detail:
            msg += f"   ({detail})"
        self.logger.info(msg)

    def _warn(self, message: str) -> None:
        """Emit a warning with the same bracketed prefix."""
        tag = f"[{self.__class__.__name__}]"
        self.logger.warning(f"{tag} ⚠ {message}")
