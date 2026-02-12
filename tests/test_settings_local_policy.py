from __future__ import annotations

from portfolio_assistant.config.settings import get_settings


def test_local_only_defaults_when_flags_unset(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_ASK_GPT", raising=False)
    monkeypatch.delenv("ENABLE_WEB_MODE", raising=False)
    monkeypatch.delenv("ENABLE_DAILY_BRIEFING", raising=False)

    settings = get_settings()

    assert settings.enable_ask_gpt is False
    assert settings.enable_web_mode is False
    assert settings.enable_daily_briefing is False
