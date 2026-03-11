"""GitHub connector readiness helpers (no outbound calls)."""

from __future__ import annotations

from dataclasses import dataclass

from ..config import CommonConfig


@dataclass(frozen=True)
class GitHubConnectorStatus:
    enabled: bool
    repo_ref_set: bool
    token_set: bool
    branch: str
    ready_for_push_ops: bool


def get_github_connector_status(cfg: CommonConfig) -> GitHubConnectorStatus:
    repo_ref_set = bool(cfg.github_repo or cfg.github_remote_url)
    ready = cfg.github_enabled and repo_ref_set and cfg.github_token_set
    return GitHubConnectorStatus(
        enabled=cfg.github_enabled,
        repo_ref_set=repo_ref_set,
        token_set=cfg.github_token_set,
        branch=cfg.github_branch,
        ready_for_push_ops=ready,
    )
