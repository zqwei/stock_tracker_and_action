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
            root / "src/portfolio_assistant/ui/streamlit/pages/05_calendar.py",
            "from portfolio_assistant.ui.streamlit.views.calendar import render_page",
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
    ]

    for path, expected_import in cases:
        content = path.read_text(encoding="utf-8")
        assert expected_import in content
        assert "render_page()" in content
