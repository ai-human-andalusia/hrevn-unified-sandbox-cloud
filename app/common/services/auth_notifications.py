from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.message import EmailMessage


@dataclass(frozen=True)
class AuthNotificationResult:
    delivery_status: str
    delivery_channel: str
    subject: str
    target_email: str
    error_detail: str | None = None


def send_smtp_notification(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    mail_from: str,
    target_email: str,
    subject: str,
    body: str,
) -> AuthNotificationResult:
    message = EmailMessage()
    message["From"] = mail_from
    message["To"] = target_email
    message["Subject"] = subject
    message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(message)
        return AuthNotificationResult(
            delivery_status="sent",
            delivery_channel="smtp",
            subject=subject,
            target_email=target_email,
            error_detail=None,
        )
    except Exception as exc:
        return AuthNotificationResult(
            delivery_status="failed",
            delivery_channel="smtp",
            subject=subject,
            target_email=target_email,
            error_detail=str(exc),
        )
