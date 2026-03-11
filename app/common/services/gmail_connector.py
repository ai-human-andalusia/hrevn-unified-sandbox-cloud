"""Gmail/SMTP readiness helpers (no outbound calls)."""

from __future__ import annotations

from dataclasses import dataclass

from ..config import CommonConfig


@dataclass(frozen=True)
class MailConnectorStatus:
    gmail_oauth_ready: bool
    smtp_ready: bool
    preferred_channel: str


def get_mail_connector_status(cfg: CommonConfig) -> MailConnectorStatus:
    gmail_ready = (
        cfg.gmail_enabled
        and cfg.gmail_client_id_set
        and cfg.gmail_client_secret_set
        and cfg.gmail_refresh_token_set
    )
    smtp_ready = cfg.smtp_enabled and bool(cfg.smtp_host) and cfg.smtp_user_set and cfg.smtp_pass_set

    preferred = "none"
    if gmail_ready:
        preferred = "gmail_oauth"
    elif smtp_ready:
        preferred = "smtp"

    return MailConnectorStatus(
        gmail_oauth_ready=gmail_ready,
        smtp_ready=smtp_ready,
        preferred_channel=preferred,
    )
