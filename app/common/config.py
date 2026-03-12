"""Unified local configuration for cross-vertical integrations.

Security model:
- Secrets are read from local environment or local .env files.
- No secrets are hardcoded in code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

ROOT = Path("/Users/miguelmiguel/CODEX/HREVN UNIFIED V1 SANDBOX")


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _read_dotenv(path: Path) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    out: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key:
            out[key] = val
    return out


def _merged_env() -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for candidate in (ROOT / ".env.example", ROOT / ".env", ROOT / ".env.local"):
        merged.update(_read_dotenv(candidate))
    # Real environment has priority over files.
    merged.update({k: v for k, v in os.environ.items() if isinstance(v, str)})
    return merged


@dataclass(frozen=True)
class CommonConfig:
    # AI providers
    ai_primary_provider: str
    openai_enabled: bool
    openai_api_key_set: bool
    openai_model: str
    openai_api_base_url: str

    gemini_enabled: bool
    gemini_api_key_set: bool
    gemini_model: str

    # Gmail / SMTP
    gmail_enabled: bool
    gmail_client_id_set: bool
    gmail_client_secret_set: bool
    gmail_refresh_token_set: bool
    gmail_mailbox_user: str
    gmail_sync_query: str

    smtp_enabled: bool
    smtp_host: str
    smtp_port: int
    smtp_user_set: bool
    smtp_pass_set: bool
    mail_from: str
    notify_email: str

    # GitHub
    github_enabled: bool
    github_repo: str
    github_remote_url: str
    github_branch: str
    github_token_set: bool

    # Telegram
    telegram_enabled: bool
    telegram_bot_token_set: bool
    telegram_chat_id_set: bool

    # Blockchain
    blockchain_enabled: bool
    blockchain_network: str
    blockchain_target: str


def load_common_config() -> CommonConfig:
    env = _merged_env()

    openai_enabled = _as_bool(env.get("OPENAI_ENABLED"), default=True)
    gemini_enabled = _as_bool(env.get("GEMINI_ENABLED"), default=False)

    return CommonConfig(
        ai_primary_provider=(env.get("AI_PRIMARY_PROVIDER", "openai").strip().lower() or "openai"),
        openai_enabled=openai_enabled,
        openai_api_key_set=bool(env.get("OPENAI_API_KEY", "").strip()),
        openai_model=env.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini",
        openai_api_base_url=env.get("OPENAI_API_BASE_URL", "https://api.openai.com/v1").strip() or "https://api.openai.com/v1",
        gemini_enabled=gemini_enabled,
        gemini_api_key_set=bool(env.get("GEMINI_API_KEY", "").strip()),
        gemini_model=env.get("GEMINI_MODEL", "gemini-1.5-flash").strip() or "gemini-1.5-flash",
        gmail_enabled=_as_bool(env.get("GMAIL_ENABLED"), default=True),
        gmail_client_id_set=bool(env.get("GMAIL_CLIENT_ID", "").strip()),
        gmail_client_secret_set=bool(env.get("GMAIL_CLIENT_SECRET", "").strip()),
        gmail_refresh_token_set=bool(env.get("GMAIL_REFRESH_TOKEN", "").strip()),
        gmail_mailbox_user=env.get("GMAIL_MAILBOX_USER", "me").strip() or "me",
        gmail_sync_query=env.get("GMAIL_SYNC_QUERY", "is:unread").strip() or "is:unread",
        smtp_enabled=_as_bool(env.get("SMTP_ENABLED"), default=True),
        smtp_host=env.get("SMTP_HOST", "").strip(),
        smtp_port=int(env.get("SMTP_PORT", "587") or "587"),
        smtp_user_set=bool(env.get("SMTP_USER", "").strip()),
        smtp_pass_set=bool(env.get("SMTP_PASS", "").strip()),
        mail_from=env.get("MAIL_FROM", "").strip(),
        notify_email=env.get("NOTIFY_EMAIL", "").strip(),
        github_enabled=_as_bool(env.get("GITHUB_ENABLED"), default=True),
        github_repo=env.get("GITHUB_REPO", "").strip(),
        github_remote_url=env.get("GITHUB_REMOTE_URL", "").strip(),
        github_branch=env.get("GITHUB_BRANCH", "main").strip() or "main",
        github_token_set=bool(env.get("GITHUB_TOKEN", "").strip()),
        telegram_enabled=_as_bool(env.get("TELEGRAM_ENABLED"), default=False),
        telegram_bot_token_set=bool(env.get("TELEGRAM_BOT_TOKEN", "").strip()),
        telegram_chat_id_set=bool(env.get("TELEGRAM_CHAT_ID", "").strip()),
        blockchain_enabled=_as_bool(env.get("BLOCKCHAIN_ENABLED"), default=True),
        blockchain_network=env.get("BLOCKCHAIN_NETWORK", "ethereum_sepolia").strip() or "ethereum_sepolia",
        blockchain_target=env.get("BLOCKCHAIN_TARGET", "sepolia").strip() or "sepolia",
    )
