from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import portfolio_assistant.assistant.daily_briefing as daily_briefing_module
from portfolio_assistant.assistant.daily_briefing import (
    generate_daily_briefing,
    list_briefing_artifacts,
    load_briefing_artifact,
)
from portfolio_assistant.config.settings import SummarizerProvider, get_settings
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
    assert payload["summary_provider_requested"] == "none"
    assert payload["summary_provider"] == "none"
    assert payload["summary_status"] == "local_deterministic"
    assert payload["summary_metrics"]["holdings_symbol_count"] == 1
    assert "Local deterministic briefing:" in payload["summary_text"]
    assert payload["holdings_context"]["symbols"] == ["MSFT"]
    assert payload["holdings_updates"]["source"] == "rss"
    assert payload["holdings_updates"]["configured_feeds"] == []
    assert payload["holdings_updates"]["item_count"] == 0
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


def test_generate_daily_briefing_enriches_holdings_aware_rss_updates(tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add_all(
            [
                PositionOpen(
                    account_id=account_id,
                    instrument_type="STOCK",
                    symbol="AAPL",
                    option_symbol_raw=None,
                    quantity=10.0,
                    avg_cost=100.0,
                    last_price=105.0,
                    market_value=1050.0,
                    unrealized_pnl=50.0,
                ),
                PositionOpen(
                    account_id=account_id,
                    instrument_type="STOCK",
                    symbol="MSFT",
                    option_symbol_raw=None,
                    quantity=5.0,
                    avg_cost=200.0,
                    last_price=198.0,
                    market_value=990.0,
                    unrealized_pnl=-10.0,
                ),
            ]
        )
        session.commit()

    feed_xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Tech Feed</title>
    <item>
      <title>AAPL product launch update</title>
      <link>https://news.example.com/story/aapl-launch?utm_source=rss</link>
      <pubDate>Mon, 09 Feb 2026 12:00:00 GMT</pubDate>
      <description>Analysts discuss AAPL demand trends.</description>
    </item>
    <item>
      <title>AMZN logistics expansion</title>
      <link>https://news.example.com/story/amzn-logistics</link>
      <pubDate>Mon, 09 Feb 2026 11:00:00 GMT</pubDate>
      <description>AMZN has no match with this account's holdings.</description>
    </item>
  </channel>
</rss>
"""

    def fetcher(_url: str) -> str:
        return feed_xml

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=False,
        rss_feed_urls=["https://feeds.example.com/tech.xml"],
        rss_lookback_days=7,
        rss_fetcher=fetcher,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 10, 9, 0, 0),
    )

    payload = result.payload
    assert payload["holdings_context"]["symbols"] == ["AAPL", "MSFT"]

    updates = payload["holdings_updates"]
    assert updates["feeds_requested"] == 1
    assert updates["feeds_ingested"] == 1
    assert updates["duplicate_feeds_skipped"] == 0
    assert updates["duplicate_items_skipped"] == 0
    assert updates["item_count"] == 1
    assert updates["errors"] == []
    assert updates["items"][0]["symbols"] == ["AAPL"]
    assert "AAPL product launch update" in updates["items"][0]["title"]


def test_settings_default_summarizer_provider_is_none(monkeypatch):
    monkeypatch.delenv("SUMMARIZER_PROVIDER", raising=False)
    settings = get_settings()
    assert settings.summarizer_provider == SummarizerProvider.NONE


def test_generate_daily_briefing_local_mode_does_not_call_openai(monkeypatch, tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add(
            PositionOpen(
                account_id=account_id,
                instrument_type="STOCK",
                symbol="AAPL",
                option_symbol_raw=None,
                quantity=3.0,
                avg_cost=100.0,
                last_price=101.0,
                market_value=303.0,
                unrealized_pnl=3.0,
            )
        )
        session.commit()

    def _raise_if_called() -> None:
        raise AssertionError("OpenAI client should not be built in local summarizer mode.")

    monkeypatch.setattr(daily_briefing_module, "_build_openai_client", _raise_if_called)

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=True,
        summarizer_provider=SummarizerProvider.NONE,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 10, 9, 0, 0),
    )

    payload = result.payload
    assert result.gpt_summary is None
    assert payload["summary_provider_requested"] == "none"
    assert payload["summary_provider"] == "none"
    assert payload["summary_status"] == "local_deterministic"
    assert "Local deterministic briefing:" in payload["summary_text"]
    assert "gpt_error" not in payload


def test_generate_daily_briefing_openai_provider_but_gpt_summary_disabled_does_not_call_openai(
    monkeypatch, tmp_path
):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add(
            PositionOpen(
                account_id=account_id,
                instrument_type="STOCK",
                symbol="AAPL",
                option_symbol_raw=None,
                quantity=1.0,
                avg_cost=100.0,
                last_price=99.0,
                market_value=99.0,
                unrealized_pnl=-1.0,
            )
        )
        session.commit()

    def _raise_if_called() -> None:
        raise AssertionError("OpenAI client should not be built when include_gpt_summary=False.")

    monkeypatch.setattr(daily_briefing_module, "_build_openai_client", _raise_if_called)

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=False,
        summarizer_provider=SummarizerProvider.OPENAI,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 10, 9, 15, 0),
    )

    payload = result.payload
    assert result.gpt_summary is None
    assert payload["summary_provider_requested"] == "openai"
    assert payload["summary_provider"] == "none"
    assert payload["summary_status"] == "local_deterministic"
    assert "gpt_error" not in payload


def test_generate_daily_briefing_openai_mode_falls_back_to_local_on_client_error(
    monkeypatch, tmp_path
):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add(
            PositionOpen(
                account_id=account_id,
                instrument_type="STOCK",
                symbol="MSFT",
                option_symbol_raw=None,
                quantity=2.0,
                avg_cost=200.0,
                last_price=198.0,
                market_value=396.0,
                unrealized_pnl=-4.0,
            )
        )
        session.commit()

    def _raise_missing_openai() -> None:
        raise RuntimeError(
            "openai package is not installed. Install `openai` to use openai summarizer mode."
        )

    monkeypatch.setattr(daily_briefing_module, "_build_openai_client", _raise_missing_openai)

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=True,
        summarizer_provider=SummarizerProvider.OPENAI,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 10, 9, 30, 0),
    )

    payload = result.payload
    assert result.gpt_summary is None
    assert payload["summary_provider_requested"] == "openai"
    assert payload["summary_provider"] == "none"
    assert payload["summary_status"] == "openai_fallback"
    assert payload["summary_fallback"] == "local_deterministic"
    assert "Local deterministic briefing:" in payload["summary_text"]
    assert "openai package is not installed" in payload["gpt_error"]


def test_generate_daily_briefing_openai_mode_success_uses_ai_summary(tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        account = _seed_account(session)
        account_id = account.id
        session.add(
            PositionOpen(
                account_id=account_id,
                instrument_type="STOCK",
                symbol="MSFT",
                option_symbol_raw=None,
                quantity=2.0,
                avg_cost=200.0,
                last_price=205.0,
                market_value=410.0,
                unrealized_pnl=10.0,
            )
        )
        session.commit()

    class _FakeResponses:
        @staticmethod
        def create(**_kwargs):
            return SimpleNamespace(output_text="AI summary text", output=[])

    fake_client = SimpleNamespace(responses=_FakeResponses())

    result = generate_daily_briefing(
        engine,
        model="gpt-5-mini",
        account_id=account_id,
        include_gpt_summary=True,
        summarizer_provider=SummarizerProvider.OPENAI,
        client=fake_client,
        output_dir=tmp_path,
        as_of=datetime(2026, 2, 10, 10, 0, 0),
    )

    payload = result.payload
    assert result.gpt_summary == "AI summary text"
    assert payload["summary_provider_requested"] == "openai"
    assert payload["summary_provider"] == "openai"
    assert payload["summary_status"] == "openai_success"
    assert payload["summary_text"] == "AI summary text"
