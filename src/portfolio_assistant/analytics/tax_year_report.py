"""Tax-year gain/loss rollups from realized P&L rows."""

from __future__ import annotations

from dataclasses import dataclass

from portfolio_assistant.db.models import AccountType, RealizedPnLRow
from portfolio_assistant.utils.money import round_money


@dataclass(slots=True)
class TaxYearReport:
    year: int
    details: list[dict[str, str | float | int | bool]]
    summary: dict[str, float]


def _term_from_holding_days(days: int) -> str:
    return "LONG" if days >= 365 else "SHORT"


def build_tax_year_report(
    realized_rows: list[RealizedPnLRow],
    year: int,
    taxable_only: bool = True,
) -> TaxYearReport:
    details: list[dict[str, str | float | int | bool]] = []
    short_adjusted = 0.0
    long_adjusted = 0.0
    wash_disallowed_total = 0.0

    for row in realized_rows:
        if row.closed_at.year != year:
            continue
        if taxable_only and row.account_type != AccountType.TAXABLE:
            continue

        disallowed = row.wash_disallowed_loss if row.is_wash_sale else 0.0
        adjusted = row.realized_pnl + disallowed
        term = _term_from_holding_days(row.holding_days)

        if term == "SHORT":
            short_adjusted += adjusted
        else:
            long_adjusted += adjusted
        wash_disallowed_total += disallowed

        details.append(
            {
                "symbol": row.symbol,
                "close_date": row.closed_at.date().isoformat(),
                "account_id": row.account_id,
                "term": term,
                "quantity": row.quantity,
                "proceeds": row.proceeds,
                "cost_basis": row.cost_basis,
                "realized_pnl": row.realized_pnl,
                "wash_sale": row.is_wash_sale,
                "wash_disallowed_loss": disallowed,
                "taxable_gain_loss": round_money(adjusted),
            }
        )

    summary = {
        "short_term_taxable_gain_loss": round_money(short_adjusted),
        "long_term_taxable_gain_loss": round_money(long_adjusted),
        "total_taxable_gain_loss": round_money(short_adjusted + long_adjusted),
        "wash_disallowed_loss": round_money(wash_disallowed_total),
    }

    details.sort(key=lambda item: (str(item["close_date"]), str(item["symbol"])))
    return TaxYearReport(year=year, details=details, summary=summary)
