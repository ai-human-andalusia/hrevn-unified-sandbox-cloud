"""AI provider selection with OpenAI-first priority and optional Gemini fallback."""

from __future__ import annotations

from dataclasses import dataclass

from ..config import CommonConfig


@dataclass(frozen=True)
class ProviderChoice:
    selected: str
    reason: str


def choose_ai_provider(cfg: CommonConfig) -> ProviderChoice:
    primary = cfg.ai_primary_provider

    if primary == "openai":
        if cfg.openai_enabled and cfg.openai_api_key_set:
            return ProviderChoice("openai", "primary_provider_ready")
        if cfg.gemini_enabled and cfg.gemini_api_key_set:
            return ProviderChoice("gemini", "openai_unavailable_fallback_gemini")
        return ProviderChoice("none", "no_enabled_ai_provider_with_credentials")

    if primary == "gemini":
        if cfg.gemini_enabled and cfg.gemini_api_key_set:
            return ProviderChoice("gemini", "primary_provider_ready")
        if cfg.openai_enabled and cfg.openai_api_key_set:
            return ProviderChoice("openai", "gemini_unavailable_fallback_openai")
        return ProviderChoice("none", "no_enabled_ai_provider_with_credentials")

    # Unknown primary value -> enforce OpenAI preference policy.
    if cfg.openai_enabled and cfg.openai_api_key_set:
        return ProviderChoice("openai", "unknown_primary_defaulted_to_openai")
    if cfg.gemini_enabled and cfg.gemini_api_key_set:
        return ProviderChoice("gemini", "unknown_primary_defaulted_with_fallback")
    return ProviderChoice("none", "no_enabled_ai_provider_with_credentials")
