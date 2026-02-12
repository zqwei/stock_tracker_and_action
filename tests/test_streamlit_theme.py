from __future__ import annotations

from portfolio_assistant.ui.streamlit import theme


def test_apply_futuristic_theme_includes_bright_overrides(monkeypatch):
    state: dict[str, str] = {}
    captured: dict[str, object] = {}

    monkeypatch.setattr(theme.st, "session_state", state)
    monkeypatch.setattr(
        theme.st,
        "markdown",
        lambda body, unsafe_allow_html=False: captured.update(
            {"body": body, "unsafe": unsafe_allow_html}
        ),
    )

    theme.apply_futuristic_theme("bright")
    css = str(captured["body"])

    assert captured["unsafe"] is True
    assert "--pa-bg_0: #edf4ff;" in css
    assert "button:focus-visible" in css
    assert "[data-testid=\"stSidebar\"] [data-testid=\"stSidebarNav\"] a span" in css
    assert "[data-testid=\"stAlertContainer\"]:has([data-testid=\"stAlertContentWarning\"])" in css


def test_apply_futuristic_theme_deep_dark_excludes_bright_only_rules(monkeypatch):
    state: dict[str, str] = {}
    captured: dict[str, object] = {}

    monkeypatch.setattr(theme.st, "session_state", state)
    monkeypatch.setattr(
        theme.st,
        "markdown",
        lambda body, unsafe_allow_html=False: captured.update(
            {"body": body, "unsafe": unsafe_allow_html}
        ),
    )

    theme.apply_futuristic_theme("deep_dark")
    css = str(captured["body"])

    assert captured["unsafe"] is True
    assert "--pa-bg_0: #000000;" in css
    assert "--pa-button_bg: #131518;" in css
    assert "--pa-button_border: #434b57;" in css
    assert "button:focus-visible" in css
    assert "[data-testid=\"stAlertContainer\"]:has([data-testid=\"stAlertContentWarning\"])" not in css
