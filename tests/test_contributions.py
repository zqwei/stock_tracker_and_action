from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from portfolio_assistant.analytics.contributions import compute_contributions
from portfolio_assistant.db.models import AccountType, CashActivity


class ContributionTests(unittest.TestCase):
    def test_external_only_net_calculation(self) -> None:
        rows = [
            CashActivity(
                broker="B",
                account_id="tax",
                account_type=AccountType.TAXABLE,
                posted_at=datetime(2025, 1, 1),
                type="DEPOSIT",
                amount=1000.0,
                is_external=True,
            ),
            CashActivity(
                broker="B",
                account_id="tax",
                account_type=AccountType.TAXABLE,
                posted_at=datetime(2025, 1, 10),
                type="WITHDRAWAL",
                amount=200.0,
                is_external=True,
            ),
            CashActivity(
                broker="B",
                account_id="ira",
                account_type=AccountType.TRAD_IRA,
                posted_at=datetime(2025, 1, 15),
                type="DEPOSIT",
                amount=500.0,
                is_external=False,
            ),
        ]

        summary = compute_contributions(rows)
        self.assertEqual(800.0, summary.net_total)
        self.assertEqual([{"month": "2025-01", "net": 800.0}], summary.by_month)


if __name__ == "__main__":
    unittest.main()
