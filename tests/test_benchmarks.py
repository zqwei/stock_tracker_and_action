from __future__ import annotations

from datetime import date, datetime
from math import isclose

from sqlalchemy.orm import Session

from portfolio_assistant.analytics.benchmarks import (
    WINDOW_LABELS,
    compute_all_window_metrics,
    compute_window_metrics,
)
from portfolio_assistant.db.models import Account, CashActivity, PriceCache, TradeNormalized


def _seed_core_account(session: Session) -> Account:
    account = Account(
        broker="B1",
        account_label="Taxable",
        account_type="TAXABLE",
    )
    session.add(account)
    session.flush()

    session.add(
        CashActivity(
            account_id=account.id,
            broker="B1",
            posted_at=datetime(2025, 1, 1, 9, 0, 0),
            activity_type="DEPOSIT",
            amount=1000.0,
            description="Initial ACH",
            source="ACH",
            is_external=True,
        )
    )
    session.add(
        TradeNormalized(
            account_id=account.id,
            broker="B1",
            executed_at=datetime(2025, 1, 2, 10, 0, 0),
            instrument_type="STOCK",
            symbol="AAPL",
            side="BUY",
            quantity=10.0,
            price=100.0,
            fees=0.0,
            net_amount=-1000.0,
            multiplier=1,
            currency="USD",
        )
    )

    session.add_all(
        [
            PriceCache(
                symbol="AAPL",
                as_of=datetime(2025, 1, 1, 16, 0, 0),
                interval="1d",
                close=100.0,
            ),
            PriceCache(
                symbol="AAPL",
                as_of=datetime(2025, 1, 31, 16, 0, 0),
                interval="1d",
                close=110.0,
            ),
            PriceCache(
                symbol="DIA",
                as_of=datetime(2025, 1, 1, 16, 0, 0),
                interval="1d",
                close=200.0,
            ),
            PriceCache(
                symbol="DIA",
                as_of=datetime(2025, 1, 31, 16, 0, 0),
                interval="1d",
                close=220.0,
            ),
            PriceCache(
                symbol="SPY",
                as_of=datetime(2025, 1, 1, 16, 0, 0),
                interval="1d",
                close=400.0,
            ),
            PriceCache(
                symbol="SPY",
                as_of=datetime(2025, 1, 31, 16, 0, 0),
                interval="1d",
                close=420.0,
            ),
            PriceCache(
                symbol="QQQ",
                as_of=datetime(2025, 1, 1, 16, 0, 0),
                interval="1d",
                close=300.0,
            ),
            PriceCache(
                symbol="QQQ",
                as_of=datetime(2025, 1, 31, 16, 0, 0),
                interval="1d",
                close=330.0,
            ),
        ]
    )
    session.flush()
    return account


def test_compute_window_metrics_since_inception_returns_expected_values(db_session: Session):
    account = _seed_core_account(db_session)

    metrics = compute_window_metrics(
        db_session,
        account_id=account.id,
        window="Since inception",
        as_of=date(2025, 1, 31),
    )

    assert metrics["start_date"] == date(2025, 1, 1)
    assert metrics["end_date"] == date(2025, 1, 31)
    assert isclose(float(metrics["start_equity"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(metrics["end_equity"]), 1100.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(metrics["external_net_flow"]), 1000.0, rel_tol=0.0, abs_tol=1e-9)
    assert isclose(float(metrics["portfolio_return"]), 0.1, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(
        float(metrics["benchmark_returns"]["DIA"]), 0.1, rel_tol=0.0, abs_tol=1e-9
    )
    assert isclose(
        float(metrics["benchmark_returns"]["SPY"]), 0.05, rel_tol=0.0, abs_tol=1e-9
    )
    assert isclose(
        float(metrics["benchmark_returns"]["QQQ"]), 0.1, rel_tol=0.0, abs_tol=1e-9
    )
    assert metrics["missing_benchmark_symbols"] == []


def test_compute_window_metrics_respects_account_scope(db_session: Session):
    account = _seed_core_account(db_session)

    other = Account(
        broker="B1",
        account_label="Roth IRA",
        account_type="ROTH_IRA",
    )
    db_session.add(other)
    db_session.flush()

    db_session.add(
        CashActivity(
            account_id=other.id,
            broker="B1",
            posted_at=datetime(2025, 1, 1, 9, 30, 0),
            activity_type="DEPOSIT",
            amount=1000.0,
            description="IRA funding",
            source="ACH",
            is_external=True,
        )
    )
    db_session.add(
        TradeNormalized(
            account_id=other.id,
            broker="B1",
            executed_at=datetime(2025, 1, 2, 10, 30, 0),
            instrument_type="STOCK",
            symbol="MSFT",
            side="BUY",
            quantity=10.0,
            price=100.0,
            fees=0.0,
            net_amount=-1000.0,
            multiplier=1,
            currency="USD",
        )
    )
    db_session.add_all(
        [
            PriceCache(
                symbol="MSFT",
                as_of=datetime(2025, 1, 1, 16, 0, 0),
                interval="1d",
                close=100.0,
            ),
            PriceCache(
                symbol="MSFT",
                as_of=datetime(2025, 1, 31, 16, 0, 0),
                interval="1d",
                close=90.0,
            ),
        ]
    )
    db_session.flush()

    scoped = compute_window_metrics(
        db_session,
        account_id=account.id,
        window="Since inception",
        as_of=date(2025, 1, 31),
    )
    consolidated = compute_window_metrics(
        db_session,
        account_id=None,
        window="Since inception",
        as_of=date(2025, 1, 31),
    )

    assert isclose(float(scoped["portfolio_return"]), 0.1, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(consolidated["portfolio_return"]), 0.0, rel_tol=0.0, abs_tol=1e-6)
    assert float(scoped["portfolio_return"]) > float(consolidated["portfolio_return"])


def test_compute_window_metrics_reports_missing_benchmark_prices(db_session: Session):
    account = Account(
        broker="B1",
        account_label="Taxable",
        account_type="TAXABLE",
    )
    db_session.add(account)
    db_session.flush()

    db_session.add(
        CashActivity(
            account_id=account.id,
            broker="B1",
            posted_at=datetime(2025, 1, 1, 9, 0, 0),
            activity_type="DEPOSIT",
            amount=100.0,
            description="Seed",
            source="ACH",
            is_external=True,
        )
    )
    db_session.add(
        PriceCache(
            symbol="DIA",
            as_of=datetime(2025, 1, 31, 16, 0, 0),
            interval="1d",
            close=220.0,
        )
    )
    db_session.add(
        PriceCache(
            symbol="SPY",
            as_of=datetime(2025, 1, 31, 16, 0, 0),
            interval="1d",
            close=420.0,
        )
    )
    db_session.flush()

    metrics = compute_window_metrics(
        db_session,
        account_id=account.id,
        window="Since inception",
        as_of=date(2025, 1, 31),
    )

    assert metrics["benchmark_returns"]["QQQ"] is None
    assert "QQQ" in metrics["missing_benchmark_symbols"]


def test_compute_all_window_metrics_returns_standard_windows(db_session: Session):
    account = _seed_core_account(db_session)

    rows = compute_all_window_metrics(
        db_session,
        account_id=account.id,
        as_of=date(2025, 1, 31),
    )

    assert [row["window"] for row in rows] == WINDOW_LABELS

