from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from portfolio_assistant.analytics.wash_sale import detect_wash_sale_risks
from portfolio_assistant.db.models import AccountType, InstrumentType, RealizedPnLRow, Trade, TradeSide


class WashSaleTests(unittest.TestCase):
    def test_detects_cross_account_replacement_buy_within_window(self) -> None:
        realized_rows = [
            RealizedPnLRow(
                symbol="AAPL",
                account_id="taxable-1",
                account_type=AccountType.TAXABLE,
                instrument_type=InstrumentType.STOCK,
                opened_at=datetime(2025, 1, 2),
                closed_at=datetime(2025, 1, 15),
                quantity=10,
                proceeds=900.0,
                cost_basis=1000.0,
                fees=1.0,
                realized_pnl=-101.0,
                holding_days=13,
                close_trade_id="sale-1",
            )
        ]
        trades = [
            Trade(
                broker="BrokerA",
                account_id="ira-1",
                account_type=AccountType.TRAD_IRA,
                account_label="IRA",
                executed_at=datetime(2025, 1, 20),
                instrument_type=InstrumentType.STOCK,
                symbol="AAPL",
                side=TradeSide.BUY,
                quantity=5,
                price=95.0,
                trade_id="buy-ira-1",
            ),
            Trade(
                broker="BrokerA",
                account_id="taxable-1",
                account_type=AccountType.TAXABLE,
                account_label="Taxable",
                executed_at=datetime(2025, 3, 1),
                instrument_type=InstrumentType.STOCK,
                symbol="AAPL",
                side=TradeSide.BUY,
                quantity=5,
                price=92.0,
                trade_id="buy-late",
            ),
        ]

        risks = detect_wash_sale_risks(realized_rows, trades)
        self.assertEqual(1, len(risks))
        self.assertEqual("AAPL", risks[0].symbol)
        self.assertEqual("ira-1", risks[0].replacement_account_id)

    def test_ignores_non_loss_sales(self) -> None:
        realized_rows = [
            RealizedPnLRow(
                symbol="MSFT",
                account_id="taxable-1",
                account_type=AccountType.TAXABLE,
                instrument_type=InstrumentType.STOCK,
                opened_at=datetime(2025, 2, 1),
                closed_at=datetime(2025, 2, 10),
                quantity=4,
                proceeds=420.0,
                cost_basis=400.0,
                fees=0.0,
                realized_pnl=20.0,
                holding_days=9,
            )
        ]
        trades = [
            Trade(
                broker="BrokerA",
                account_id="ira-1",
                account_type=AccountType.TRAD_IRA,
                account_label="IRA",
                executed_at=datetime(2025, 2, 9),
                instrument_type=InstrumentType.STOCK,
                symbol="MSFT",
                side=TradeSide.BUY,
                quantity=1,
                price=100.0,
            )
        ]

        risks = detect_wash_sale_risks(realized_rows, trades)
        self.assertEqual([], risks)


if __name__ == "__main__":
    unittest.main()
