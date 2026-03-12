"""AI-backed Real Estate pre-issuance review for the unified sandbox."""

from __future__ import annotations

import base64
import json
import mimetypes
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RealEstateAIReviewResult:
    decision: str
    approved: bool
    provider: str
    model: str
    execution_mode: str
    review_mode: str
    reviewed_at_utc: str
    summary: str
    blocking_reasons: list[str]
    semantic_titles: list[dict[str, str]]
    reviewed_observation_count: int
    reviewed_photo_count: int
    target_blocking_policy: str
    anchor_status: str
    anchor_target: str
    ai_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _clean(value: Any) -> str:
    return str(value or '').strip()


def _candidate_image_paths(photos: list[dict[str, Any]]) -> list[Path]:
    out: list[Path] = []
    for photo in photos:
        for raw in (photo.get('photo_path'), photo.get('photo_relpath')):
            candidate = Path(_clean(raw))
            if candidate and candidate.exists() and candidate.is_file():
                out.append(candidate)
                break
    return out[:2]


def _build_semantic_titles(asset_public_id: str, visit_id: str, photos: list[dict[str, Any]]) -> list[dict[str, str]]:
    titles: list[dict[str, str]] = []
    for idx, photo in enumerate(photos[:5], start=1):
        role = _clean(photo.get('photo_role')) or f'evidence_{idx}'
        titles.append(
            {
                'photo_uuid': _clean(photo.get('photo_uuid')),
                'photo_filename': _clean(photo.get('photo_filename')),
                'title': f"{asset_public_id or 'asset'} / {visit_id or 'visit'} / {role.lower().replace('_', ' ')}",
            }
        )
    return titles


def _deterministic_reasons(workspace: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not bool(workspace.get('all_observations_have_lpi')):
        reasons.append('One or more observations do not have an LPI code assigned.')
    if int(workspace.get('total_present_photos') or 0) < int(workspace.get('total_required_photos') or 0):
        reasons.append('The visit does not yet meet the minimum required photo count.')
    for photo in workspace.get('photos') or []:
        flags = _clean(photo.get('quality_flags')).lower()
        if any(token in flags for token in ('blur', 'blurry', 'dark', 'low', 'cropped', 'invalid')):
            reasons.append(f"Photo {photo.get('photo_filename') or photo.get('photo_uuid')} carries quality flags: {photo.get('quality_flags')}")
    # keep order, remove duplicates
    seen = set()
    deduped: list[str] = []
    for item in reasons:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def _extract_json_object(text: str) -> dict[str, Any] | None:
    text = text.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r'\{.*\}', text, flags=re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def _encode_image_data_url(path: Path) -> str:
    mime = mimetypes.guess_type(path.name)[0] or 'image/jpeg'
    data = base64.b64encode(path.read_bytes()).decode('ascii')
    return f'data:{mime};base64,{data}'


def _call_openai_review(
    *,
    api_key: str,
    model: str,
    base_url: str,
    payload_context: dict[str, Any],
    image_paths: list[Path],
) -> tuple[dict[str, Any] | None, str | None]:
    system_prompt = (
        'You are a strict institutional reviewer for H-REVN certification requests. '
        'Return ONLY valid JSON with keys decision, summary, blocking_reasons, semantic_titles, confidence. '
        'Decision must be approve or review_required. '
        'block_reasons must be an array of short strings. '
        'semantic_titles must be an array of objects with photo_uuid and title.'
    )
    user_text = (
        'Review this certification request. Block the request if the evidence is insufficient, inconsistent, or quality concerns are present. '
        'If images are absent, review the structured evidence only and say so in the summary.\n\n'
        f'{json.dumps(payload_context, ensure_ascii=False, indent=2)}'
    )
    content: list[dict[str, Any]] = [{"type": "text", "text": user_text}]
    for path in image_paths:
        try:
            content.append({"type": "image_url", "image_url": {"url": _encode_image_data_url(path)}})
        except Exception:
            continue

    request_body = {
        'model': model,
        'temperature': 0,
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': content},
        ],
    }
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=json.dumps(request_body).encode('utf-8'),
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            raw = response.read().decode('utf-8')
        parsed = json.loads(raw)
        content_text = (
            (((parsed.get('choices') or [{}])[0].get('message') or {}).get('content'))
            or ''
        )
        if isinstance(content_text, list):
            content_text = ''.join(str(item.get('text') or '') for item in content_text if isinstance(item, dict))
        data = _extract_json_object(str(content_text))
        if not isinstance(data, dict):
            return None, 'openai_returned_non_json_payload'
        return data, None
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode('utf-8', errors='replace')
        except Exception:
            detail = str(exc)
        return None, f'openai_http_error:{exc.code}:{detail[:400]}'
    except Exception as exc:
        return None, f'openai_request_error:{exc}'


def review_real_estate_certification(
    *,
    workspace: dict[str, Any],
    provider: str,
    model: str,
    openai_api_key: str,
    openai_api_base_url: str,
    blockchain_target: str,
    blockchain_enabled: bool,
) -> RealEstateAIReviewResult:
    visit = workspace.get('visit') or {}
    asset = workspace.get('asset') or {}
    observations = list(workspace.get('observations') or [])
    photos = list(workspace.get('photos') or [])
    asset_public_id = _clean(asset.get('asset_public_id')) or _clean(visit.get('asset_id'))
    visit_id = _clean(visit.get('visit_id'))

    deterministic_reasons = _deterministic_reasons(workspace)
    semantic_titles = _build_semantic_titles(asset_public_id, visit_id, photos)
    image_paths = _candidate_image_paths(photos)
    review_mode = 'image_review' if image_paths else 'evidence_review'
    execution_mode = 'deterministic_fallback'
    ai_error = None
    ai_summary = ''
    ai_reasons: list[str] = []

    if provider == 'openai' and openai_api_key.strip():
        payload_context = {
            'visit_id': visit_id,
            'asset_public_id': asset_public_id,
            'asset_type': _clean(asset.get('asset_type') or asset.get('asset_template_type')),
            'city': _clean(asset.get('asset_city')),
            'client_name': _clean(asset.get('client_name')),
            'review_mode': review_mode,
            'blocking_policy': 'block_if_inconsistencies_detected',
            'required_photos_total': int(workspace.get('total_required_photos') or 0),
            'present_photos_total': int(workspace.get('total_present_photos') or 0),
            'all_observations_have_lpi': bool(workspace.get('all_observations_have_lpi')),
            'observation_count': len(observations),
            'photo_count': len(photos),
            'observations': [
                {
                    'record_uuid': _clean(item.get('record_uuid')),
                    'lpi_code': _clean(item.get('lpi_code')),
                    'lpi_title': _clean(item.get('lpi_title')),
                    'severity_0_5': int(item.get('severity_0_5') or 0),
                    'min_photos_required': int(item.get('min_photos_required') or 0),
                    'review_status': _clean(item.get('review_status')),
                    'row_status': _clean(item.get('row_status')),
                }
                for item in observations[:12]
            ],
            'photos': [
                {
                    'photo_uuid': _clean(item.get('photo_uuid')),
                    'record_uuid': _clean(item.get('record_uuid')),
                    'photo_role': _clean(item.get('photo_role')),
                    'photo_filename': _clean(item.get('photo_filename')),
                    'quality_flags': _clean(item.get('quality_flags')),
                }
                for item in photos[:12]
            ],
        }
        ai_data, ai_error = _call_openai_review(
            api_key=openai_api_key,
            model=model,
            base_url=openai_api_base_url,
            payload_context=payload_context,
            image_paths=image_paths,
        )
        if isinstance(ai_data, dict):
            execution_mode = 'live_openai'
            ai_summary = _clean(ai_data.get('summary'))
            ai_reasons = [str(x).strip() for x in (ai_data.get('blocking_reasons') or []) if str(x).strip()]
            ai_titles = ai_data.get('semantic_titles') or []
            if isinstance(ai_titles, list) and ai_titles:
                semantic_titles = [
                    {
                        'photo_uuid': _clean(item.get('photo_uuid')),
                        'title': _clean(item.get('title')),
                    }
                    for item in ai_titles[:5]
                    if isinstance(item, dict)
                ] or semantic_titles

    combined_reasons = []
    for item in deterministic_reasons + ai_reasons:
        if item and item not in combined_reasons:
            combined_reasons.append(item)

    approved = not combined_reasons
    decision = 'approved' if approved else 'review_required'

    if approved:
        summary = ai_summary or (
            'AI review completed with no blocking inconsistencies. The certification request can move to issuance.'
            if execution_mode == 'live_openai'
            else 'Structured evidence review completed with no blocking inconsistencies. The certification request can move to issuance.'
        )
    else:
        summary = ai_summary or (
            'The certification request requires manual review before issuance because one or more evidence controls failed.'
        )

    if review_mode == 'evidence_review' and 'structured evidence only' not in summary.lower() and 'image' not in summary.lower():
        summary = f"{summary} Review mode: structured evidence only; no image binaries were available in this environment."

    anchor_status = 'configured_target_pending_wallet' if (approved and blockchain_enabled and blockchain_target) else 'not_attempted'

    return RealEstateAIReviewResult(
        decision=decision,
        approved=approved,
        provider=provider or 'none',
        model=model,
        execution_mode=execution_mode,
        review_mode=review_mode,
        reviewed_at_utc=_utc_now(),
        summary=summary,
        blocking_reasons=combined_reasons,
        semantic_titles=semantic_titles,
        reviewed_observation_count=len(observations),
        reviewed_photo_count=len(photos),
        target_blocking_policy='block_if_inconsistencies_detected',
        anchor_status=anchor_status,
        anchor_target=blockchain_target,
        ai_error=ai_error,
    )
