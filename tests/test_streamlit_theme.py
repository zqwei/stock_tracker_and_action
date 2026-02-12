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
    assert "[data-baseweb=\"tab\"] {" in css
    assert "#f6fbff !important" in css
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
    assert "[data-baseweb=\"tab\"] {" in css
    assert "#f6fbff !important" not in css
    assert "[data-testid=\"stAlertContainer\"]:has([data-testid=\"stAlertContentWarning\"])" not in css


def test_render_theme_selector_uses_radio_not_selectbox(monkeypatch):
    state: dict[str, str] = {}
    captured: dict[str, object] = {}

    monkeypatch.setattr(theme.st, "session_state", state)
    monkeypatch.setattr(
        theme.st,
        "radio",
        lambda label, options, horizontal, format_func, key, help: (
            captured.update(
                {
                    "label": label,
                    "options": list(options),
                    "horizontal": horizontal,
                    "formatted_first": format_func(options[0]),
                    "key": key,
                    "help": help,
                }
            )
            or options[0]
        ),
    )
    monkeypatch.setattr(
        theme.st,
        "selectbox",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("selectbox not expected")),
    )

    selected = theme.render_theme_selector()

    assert selected == "bright"
    assert state[theme.UI_THEME_SESSION_KEY] == theme.DEFAULT_THEME_PRESET
    assert captured["label"] == "Color theme"
    assert captured["options"] == ["bright", "dark", "deep_dark", "palenight"]
    assert captured["horizontal"] is True
    assert captured["formatted_first"] == "Bright"
    assert captured["key"] == theme.UI_THEME_SESSION_KEY
