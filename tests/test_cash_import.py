from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from portfolio_assistant.db.models import Account, AccountType
from portfolio_assistant.ingest.cash_import import import_cash_csv


class CashImportTests(unittest.TestCase):
    def test_import_cash_csv_infers_required_fields(self) -> None:
        account = Account(
            account_id="tax-1",
            account_label="Taxable",
            broker="Demo",
            account_type=AccountType.TAXABLE,
        )

        with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False) as handle:
            writer = csv.DictWriter(handle, fieldnames=["Date", "Activity", "Amount", "Description", "Source"])
            writer.writeheader()
            writer.writerow(
                {
                    "Date": "2025-01-05",
                    "Activity": "Deposit",
                    "Amount": "1000",
                    "Description": "Bank ACH",
                    "Source": "ACH",
                }
            )
            writer.writerow(
                {
                    "Date": "2025-01-08",
                    "Activity": "Withdrawal",
                    "Amount": "250",
                    "Description": "Transfer between accounts",
                    "Source": "transfer",
                }
            )
            temp_path = handle.name

        try:
            result = import_cash_csv(temp_path, account)
            self.assertEqual([], result.unmapped_required)
            self.assertEqual(2, len(result.activities))
            self.assertEqual("DEPOSIT", result.activities[0].type)
            self.assertTrue(result.activities[0].is_external)
            self.assertFalse(result.activities[1].is_external)
        finally:
            Path(temp_path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
