from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from portfolio_assistant.assistant.daily_briefing import (
    generate_daily_briefing,
    list_briefing_artifacts,
    load_briefing_artifact,
)
from portfolio_assistant.db.models import Account, Base, CashActivity, PnlRealized, PositionOpen


def _seed_account(session: Session) -> Account:
    account = Account(broker="B1", account_label="Taxable", account_type="TAXABLE")
    session.add(account)
    session.flush()
    return account


def test_generate_daily_briefing_writes_artifact_and_guardrails(tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add(
            CashActivity(
                account_id=account_id,
                broker="B1",
                posted_at=datetime(2025, 2, 1, 9, 0, 0),
                activity_type="DEPOSIT",
                amount=1000.0,
                description="ACH transfer",
                source="ACH",
                is_external=None,
            )
        )
        session.add(
            PnlRealized(
                account_id=account_id,
                symbol="AAPL",
                instrument_type="STOCK",
                close_date=datetime(2025, 2, 2).date(),
                quantity=5.0,
                proceeds=450.0,
                cost_basis=500.0,
                fees=0.0,
                pnl=-50.0,
                notes="FIFO close from 2025-01-01",
            )
        )
        session.add(
            PositionOpen(
                account_id=account_id,
                instrument_type="STOCK",
                symbol="MSFT",
                option_symbol_raw=None,
                quantity=5.0,
                avg_cost=100.0,
                last_price=90.0,
                market_value=450.0,
                unrealized_pnl=-50.0,
            )
        )
        session.commit()

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=False,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 9, 12, 0, 0),
    )

    assert result.artifact_path.exists()
    payload = load_briefing_artifact(result.artifact_path)
    assert payload["guardrails"]["credentials_storage"] == "forbidden"
    assert payload["guardrails"]["auto_trading"] == "forbidden"
    assert payload["account_scope"] == account_id
    assert any(check["key"] == "cash_external_tagging" for check in payload["risk_checks"])


def test_list_briefing_artifacts_orders_latest_first(tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.commit()

    older = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=False,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 8, 9, 0, 0),
    )
    newer = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=False,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 9, 9, 0, 0),
    )

    files = list_briefing_artifacts(base_dir=tmp_path, limit=10)
    assert len(files) == 2
    assert files[0] == newer.artifact_path
    assert files[1] == older.artifact_path
