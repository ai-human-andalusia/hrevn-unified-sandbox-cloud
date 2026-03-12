"""Security helpers for local-secret posture."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from .config import CommonConfig


@dataclass(frozen=True)
class SecretPosture:
    openai_ok: bool
    gemini_ok: bool
    gmail_oauth_ok: bool
    smtp_ok: bool
    github_ok: bool
    telegram_ok: bool


def evaluate_secret_posture(cfg: CommonConfig) -> SecretPosture:
    return SecretPosture(
        openai_ok=(not cfg.openai_enabled) or cfg.openai_api_key_set,
        gemini_ok=(not cfg.gemini_enabled) or cfg.gemini_api_key_set,
        gmail_oauth_ok=(
            (not cfg.gmail_enabled)
            or (cfg.gmail_client_id_set and cfg.gmail_client_secret_set and cfg.gmail_refresh_token_set)
        ),
        smtp_ok=(not cfg.smtp_enabled) or (bool(cfg.smtp_host) and cfg.smtp_user_set and cfg.smtp_pass_set),
        github_ok=(not cfg.github_enabled) or bool(cfg.github_remote_url or cfg.github_repo),
        telegram_ok=(not cfg.telegram_enabled) or (cfg.telegram_bot_token_set and cfg.telegram_chat_id_set),
    )


def redact_config_for_ui(cfg: CommonConfig) -> Dict[str, object]:
    return {
        "ai_primary_provider": cfg.ai_primary_provider,
        "openai_enabled": cfg.openai_enabled,
        "openai_api_key_set": cfg.openai_api_key_set,
        "openai_model": cfg.openai_model,
        "gemini_enabled": cfg.gemini_enabled,
        "gemini_api_key_set": cfg.gemini_api_key_set,
        "gemini_model": cfg.gemini_model,
        "gmail_enabled": cfg.gmail_enabled,
        "gmail_oauth_ready": cfg.gmail_client_id_set and cfg.gmail_client_secret_set and cfg.gmail_refresh_token_set,
        "gmail_sync_query": cfg.gmail_sync_query,
        "smtp_enabled": cfg.smtp_enabled,
        "smtp_ready": bool(cfg.smtp_host) and cfg.smtp_user_set and cfg.smtp_pass_set,
        "mail_from_set": bool(cfg.mail_from),
        "notify_email_set": bool(cfg.notify_email),
        "github_enabled": cfg.github_enabled,
        "github_repo": cfg.github_repo,
        "github_remote_url_set": bool(cfg.github_remote_url),
        "github_branch": cfg.github_branch,
        "github_token_set": cfg.github_token_set,
        "telegram_enabled": cfg.telegram_enabled,
        "telegram_bot_token_set": cfg.telegram_bot_token_set,
        "telegram_chat_id_set": cfg.telegram_chat_id_set,
        "telegram_ready": cfg.telegram_bot_token_set and cfg.telegram_chat_id_set,
        "blockchain_enabled": cfg.blockchain_enabled,
        "blockchain_network": cfg.blockchain_network,
        "blockchain_target": cfg.blockchain_target,
    }
