from __future__ import annotations

from pathlib import Path


def test_phase3_streamlit_pages_are_wired_to_views():
    root = Path(__file__).resolve().parents[1]
    cases = [
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/01_overview.py",
            "from portfolio_assistant.ui.streamlit.views.overview import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/02_holdings.py",
            "from portfolio_assistant.ui.streamlit.views.holdings import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/03_pnl.py",
            "from portfolio_assistant.ui.streamlit.views.pnl import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/04_contributions.py",
            "from portfolio_assistant.ui.streamlit.views.contributions import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/05_calendar.py",
            "from portfolio_assistant.ui.streamlit.views.calendar import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/06_tax_year.py",
            "from portfolio_assistant.ui.streamlit.views.tax_year import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/07_reconciliation.py",
            "from portfolio_assistant.ui.streamlit.views.reconciliation import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/08_wash_sale_risk.py",
            "from portfolio_assistant.ui.streamlit.views.wash_sale_risk import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/09_ask_gpt.py",
            "from portfolio_assistant.ui.streamlit.views.ask_gpt import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/10_briefings.py",
            "from portfolio_assistant.ui.streamlit.views.briefings import render_page",
        ),
        (
            root / "src/portfolio_assistant/ui/streamlit/pages/11_settings.py",
            "from portfolio_assistant.ui.streamlit.views.settings import render_page",
        ),
    ]

    for path, expected_import in cases:
        content = path.read_text(encoding="utf-8")
        assert expected_import in content
        assert "render_page()" in content
