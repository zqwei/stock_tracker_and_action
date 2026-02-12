from __future__ import annotations

from pathlib import Path

from portfolio_assistant.config.settings import SummarizerProvider, get_settings


def test_local_only_defaults_when_flags_unset(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_ASK_GPT", raising=False)
    monkeypatch.delenv("ENABLE_WEB_MODE", raising=False)
    monkeypatch.delenv("ENABLE_DAILY_BRIEFING", raising=False)
    monkeypatch.delenv("SUMMARIZER_PROVIDER", raising=False)

    settings = get_settings()

    assert settings.enable_ask_gpt is False
    assert settings.enable_web_mode is False
    assert settings.enable_daily_briefing is False
    assert settings.summarizer_provider == SummarizerProvider.NONE


def test_summarizer_provider_openai_must_be_explicit(monkeypatch) -> None:
    monkeypatch.setenv("SUMMARIZER_PROVIDER", "openai")
    assert get_settings().summarizer_provider == SummarizerProvider.OPENAI

    monkeypatch.setenv("SUMMARIZER_PROVIDER", "invalid-provider")
    assert get_settings().summarizer_provider == SummarizerProvider.NONE


def test_default_manifests_do_not_require_openai_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    requirements = (root / "requirements.txt").read_text(encoding="utf-8").lower()
    requirements_dev = (root / "requirements-dev.txt").read_text(encoding="utf-8").lower()
    environment = (root / "environment.yml").read_text(encoding="utf-8").lower()

    assert "openai" not in requirements
    assert "openai-agents" not in requirements_dev
    assert "openai-agents" not in environment
