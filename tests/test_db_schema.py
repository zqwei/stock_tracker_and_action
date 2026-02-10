from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from portfolio_assistant.db.migrate import migrate
from portfolio_assistant.db.models import Base


def test_schema_has_tax_recon_and_feed_tables():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    table_names = set(inspect(engine).get_table_names())
    expected = {
        "wash_sale_adjustments",
        "reconciliation_runs",
        "reconciliation_artifacts",
        "feed_sources",
        "feed_ingest_runs",
        "feed_items",
    }
    assert expected.issubset(table_names)


def test_schema_has_disposal_metadata_columns_on_pnl_realized():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    columns = {col["name"] for col in inspect(engine).get_columns("pnl_realized")}
    expected = {
        "disposal_label",
        "security_id",
        "acquired_date",
        "disposal_term",
        "close_trade_row_id",
        "loss_trade_row_id",
        "lot_method",
        "adjustment_codes",
        "adjustment_amount",
        "wash_sale_disallowed",
        "disposal_metadata",
    }
    assert expected.issubset(columns)


def test_migrate_upgrades_legacy_sqlite_with_disposal_columns_and_indexes(tmp_path):
    db_path = tmp_path / "legacy.sqlite"
    legacy_engine = create_engine(f"sqlite:///{db_path}", future=True)
    with legacy_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE pnl_realized (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id VARCHAR(36) NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    instrument_type VARCHAR(16) NOT NULL,
                    close_date DATE NOT NULL,
                    quantity FLOAT NOT NULL,
                    proceeds FLOAT NOT NULL,
                    cost_basis FLOAT NOT NULL,
                    fees FLOAT NOT NULL DEFAULT 0.0,
                    pnl FLOAT NOT NULL,
                    notes TEXT
                )
                """
            )
        )

    migrate(database_url=f"sqlite:///{db_path}")

    engine = create_engine(f"sqlite:///{db_path}", future=True)
    inspector = inspect(engine)

    columns = {col["name"] for col in inspector.get_columns("pnl_realized")}
    assert {
        "disposal_label",
        "security_id",
        "acquired_date",
        "disposal_term",
        "close_trade_row_id",
        "loss_trade_row_id",
        "lot_method",
        "adjustment_codes",
        "adjustment_amount",
        "wash_sale_disallowed",
        "disposal_metadata",
    }.issubset(columns)

    table_names = set(inspector.get_table_names())
    assert {
        "wash_sale_adjustments",
        "reconciliation_runs",
        "reconciliation_artifacts",
        "feed_sources",
        "feed_ingest_runs",
        "feed_items",
    }.issubset(table_names)

    with engine.begin() as conn:
        index_names = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type = 'index'")
            ).fetchall()
            if row[0]
        }

    assert {
        "ix_pnl_realized_disposal_term_close",
        "ix_pnl_realized_security_close",
        "ix_wash_sale_adjustments_mode_tax_year",
        "ix_reconciliation_artifacts_run_type",
        "ix_feed_sources_type_provider",
        "ix_feed_items_source_published",
    }.issubset(index_names)
