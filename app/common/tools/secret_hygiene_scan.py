"""Local secret hygiene scan for sandbox files.

This scanner only inspects local files and returns redacted findings.
No network calls, no external services.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True)
class HygieneFinding:
    file_path: str
    line_number: int
    category: str
    snippet: str


SECRET_PATTERNS = [
    ("openai_key_literal", re.compile(r"sk-[A-Za-z0-9]{20,}")),
    ("gemini_key_literal", re.compile(r"AIza[0-9A-Za-z\\-_]{20,}")),
    ("github_token_literal", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
    ("telegram_bot_token_literal", re.compile(r"\\b\\d{8,12}:[A-Za-z0-9_-]{25,}\\b")),
    ("generic_bearer_literal", re.compile(r"Bearer\\s+[A-Za-z0-9\\-\\._]{20,}")),
]

DEFAULT_EXCLUDE = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    ".mypy_cache",
}


def _iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in DEFAULT_EXCLUDE for part in path.parts):
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip"}:
            continue
        yield path


def _redact(line: str) -> str:
    trimmed = line.strip()
    if len(trimmed) <= 80:
        return trimmed
    return f"{trimmed[:64]}...{trimmed[-12:]}"


def run_secret_hygiene_scan(root: Path, max_findings: int = 25) -> List[HygieneFinding]:
    findings: List[HygieneFinding] = []
    for file_path in _iter_files(root):
        if file_path.name in {".env", ".env.local"}:
            # Local secret files are expected; skip to avoid false positives.
            continue
        try:
            lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for idx, line in enumerate(lines, start=1):
            for category, pattern in SECRET_PATTERNS:
                if pattern.search(line):
                    findings.append(
                        HygieneFinding(
                            file_path=str(file_path),
                            line_number=idx,
                            category=category,
                            snippet=_redact(line),
                        )
                    )
                    if len(findings) >= max_findings:
                        return findings
                    break
    return findings
