from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from portfolio_assistant.analytics.tax_year_report import build_tax_year_report
from portfolio_assistant.db.models import AccountType, InstrumentType, RealizedPnLRow


class TaxYearReportTests(unittest.TestCase):
    def test_builds_short_and_long_term_summary(self) -> None:
        rows = [
            RealizedPnLRow(
                symbol="AAPL",
                account_id="taxable-1",
                account_type=AccountType.TAXABLE,
                instrument_type=InstrumentType.STOCK,
                opened_at=datetime(2024, 12, 20),
                closed_at=datetime(2025, 1, 15),
                quantity=10,
                proceeds=1200.0,
                cost_basis=1000.0,
                fees=2.0,
                realized_pnl=198.0,
                holding_days=26,
            ),
            RealizedPnLRow(
                symbol="TSLA",
                account_id="taxable-1",
                account_type=AccountType.TAXABLE,
                instrument_type=InstrumentType.STOCK,
                opened_at=datetime(2023, 10, 1),
                closed_at=datetime(2025, 1, 20),
                quantity=5,
                proceeds=1000.0,
                cost_basis=1200.0,
                fees=1.0,
                realized_pnl=-201.0,
                holding_days=477,
                is_wash_sale=True,
                wash_disallowed_loss=50.0,
            ),
            RealizedPnLRow(
                symbol="QQQ",
                account_id="ira-1",
                account_type=AccountType.TRAD_IRA,
                instrument_type=InstrumentType.STOCK,
                opened_at=datetime(2024, 1, 1),
                closed_at=datetime(2025, 1, 20),
                quantity=2,
                proceeds=800.0,
                cost_basis=700.0,
                fees=0.0,
                realized_pnl=100.0,
                holding_days=385,
            ),
        ]

        report = build_tax_year_report(rows, 2025, taxable_only=True)

        self.assertEqual(198.0, report.summary["short_term_taxable_gain_loss"])
        self.assertEqual(-151.0, report.summary["long_term_taxable_gain_loss"])
        self.assertEqual(47.0, report.summary["total_taxable_gain_loss"])
        self.assertEqual(50.0, report.summary["wash_disallowed_loss"])
        self.assertEqual(2, len(report.details))


if __name__ == "__main__":
    unittest.main()
