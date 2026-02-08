"""Local assistant facade.

Real API integration is intentionally deferred; this module gives deterministic behavior
for local-only mode during MVP scaffolding.
"""

from __future__ import annotations


def answer_question(question: str, web_enabled: bool = False) -> str:
    mode = "web-enabled" if web_enabled else "local-only"
    return (
        f"Assistant ({mode}) received: {question}. "
        "Live LLM integration is not configured in this baseline build."
    )
