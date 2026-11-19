"""Multi-source local quota and reset-time extraction for ChatGPT/Codex, Claude, and Antigravity."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from deskbell.store import BusStore

PLATFORM_TO_PEER = {
    "antigravity": "antigravity",
    "gemini": "antigravity",
    "claude": "claude",
    "claude_code": "claude",
    "anthropic": "claude",
    "codex": "chatgpt",
    "chatgpt": "chatgpt",
    "openai": "chatgpt",
}


def normalize_ts_to_iso(value: Any) -> str:
    """Normalize Unix s/ms or RFC 3339/ISO-8601 timestamps to UTC '%Y-%m-%dT%H:%M:%SZ'."""
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts <= 0:
            return ""
        if ts > 1e11:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    text = str(value).strip()
    if not text:
        return ""
    try:
        num = float(text)
    except ValueError:
        pass
    else:
        return normalize_ts_to_iso(num)

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_remaining_pct(
    remaining: Any = None,
    used: Any = None,
    *,
    is_fraction: bool = False,
) -> Optional[float]:
    """Normalize remaining or used metrics into a clamped [0.0, 100.0] percentage."""
    if remaining is not None and not isinstance(remaining, bool):
        try:
            val = float(remaining)
        except (TypeError, ValueError):
            return None
        if is_fraction:
            val *= 100.0
        return round(max(0.0, min(100.0, val)), 2)

    if used is not None and not isinstance(used, bool):
        try:
            val = float(used)
        except (TypeError, ValueError):
            return None
        if is_fraction:
            val *= 100.0
        return round(max(0.0, min(100.0, 100.0 - val)), 2)

    return None


def _connect_sqlite_ro(db_path: str) -> sqlite3.Connection:
    uri = f"file:{urllib.parse.quote(os.path.abspath(db_path))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    conn.row_factory = sqlite3.Row
    return conn


def _collect_cockpit_sqlite(cockpit_dir: str) -> List[Dict[str, Any]]:
    """Read cached quota observations from Cockpit Tools SQLite databases."""
    records: List[Dict[str, Any]] = []

    platform_db = os.path.join(cockpit_dir, "platform-quota-history-v1.sqlite")
    if os.path.isfile(platform_db):
        try:
            with _connect_sqlite_ro(platform_db) as conn:
                rows = conn.execute(
                    """
                    SELECT platform, account_id, metric, label, observed_at, remaining, reset_at
                    FROM quota_observations
                    ORDER BY observed_at DESC
                    """
                ).fetchall()
            seen: set = set()
            for row in rows:
                peer = PLATFORM_TO_PEER.get(str(row["platform"] or "").strip().lower())
                if not peer:
                    continue
                acct = str(row["account_id"] or "default")
                metric = str(row["metric"] or "default")
                key = (peer, acct, metric)
                if key in seen:
                    continue
                seen.add(key)
                pct = normalize_remaining_pct(remaining=row["remaining"])
                if pct is None:
                    continue
                records.append(
                    {
                        "peer_id": peer,
                        "account_id": acct,
                        "window_key": metric,
                        "label": str(row["label"] or metric),
                        "remaining_percent": pct,
                        "reset_at": normalize_ts_to_iso(row["reset_at"]),
                        "source": "cockpit_sqlite",
                        "observed_at": normalize_ts_to_iso(row["observed_at"]),
                    }
                )
        except (OSError, sqlite3.Error, ValueError):
            pass

    codex_db = os.path.join(cockpit_dir, "codex-quota-history-v1.sqlite")
    if os.path.isfile(codex_db):
        try:
            with _connect_sqlite_ro(codex_db) as conn:
                rows = conn.execute(
                    """
                    SELECT account_id, identity, observed_at, short_remaining, weekly_remaining
                    FROM quota_samples
                    ORDER BY observed_at DESC
                    """
                ).fetchall()
            seen_codex: set = set()
            for row in rows:
                acct = str(row["identity"] or row["account_id"] or "default")
                if acct in seen_codex:
                    continue
                seen_codex.add(acct)
                obs = normalize_ts_to_iso(row["observed_at"])
                for col, wkey, label in (
                    ("short_remaining", "5h", "5h rolling"),
                    ("weekly_remaining", "7d", "7d weekly"),
                ):
                    pct = normalize_remaining_pct(remaining=row[col])
                    if pct is not None:
                        records.append(
                            {
                                "peer_id": "chatgpt",
                                "account_id": acct,
                                "window_key": wkey,
                                "label": label,
                                "remaining_percent": pct,
                                "reset_at": "",
                                "source": "cockpit_codex_sqlite",
                                "observed_at": obs,
                            }
                        )
        except (OSError, sqlite3.Error, ValueError):
            pass

    return records


def _extract_window_from_mapping(
    peer: str,
    account_id: str,
    window_key: str,
    obj: Dict[str, Any],
    *,
    source: str,
    observed_at: str,
) -> Optional[Dict[str, Any]]:
    is_frac = "remainingFraction" in obj or "remaining_fraction" in obj
    rem = (
        obj.get("remaining_percent")
        if obj.get("remaining_percent") is not None
        else obj.get("remaining_percentage")
    )
    if rem is None:
        rem = obj.get("remaining")
    if rem is None:
        rem = obj.get("remainingFraction", obj.get("remaining_fraction"))
    used = obj.get("used_percent", obj.get("utilization"))
    pct = normalize_remaining_pct(remaining=rem, used=used, is_fraction=is_frac)
    if pct is None:
        return None

    reset_raw = (
        obj.get("reset_at")
        or obj.get("resets_at")
        or obj.get("reset_time")
        or obj.get("resetTime")
        or obj.get("next_reset_time")
        or obj.get("nextResetTime")
    )
    return {
        "peer_id": peer,
        "account_id": account_id,
        "window_key": window_key,
        "label": str(obj.get("label") or window_key),
        "remaining_percent": pct,
        "reset_at": normalize_ts_to_iso(reset_raw),
        "source": source,
        "observed_at": observed_at,
    }


def _collect_cockpit_json(cockpit_dir: str) -> List[Dict[str, Any]]:
    """Inspect Cockpit Tools account JSON caches for quota & reset fields."""
    records: List[Dict[str, Any]] = []
    file_map = (
        ("codex_accounts.json", "chatgpt"),
        ("claude_accounts.json", "claude"),
        ("google_accounts.json", "antigravity"),
        ("accounts.json", "antigravity"),
    )
    for fname, peer in file_map:
        path = os.path.join(cockpit_dir, fname)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, ValueError):
            continue

        entries: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            entries = [e for e in raw if isinstance(e, dict)]
        elif isinstance(raw, dict):
            if isinstance(raw.get("accounts"), list):
                entries = [e for e in raw["accounts"] if isinstance(e, dict)]
            else:
                entries = [v for v in raw.values() if isinstance(v, dict)]

        for entry in entries:
            acct = str(
                entry.get("email") or entry.get("account_id") or entry.get("id") or "default"
            )
            obs = normalize_ts_to_iso(
                entry.get("updated_at") or entry.get("fetchedAt") or entry.get("fetched_at")
            )
            quota_container = entry.get("quota") or entry.get("quotas") or entry.get("quota_info")
            if isinstance(quota_container, dict):
                if isinstance(quota_container.get("models"), list):
                    for model_item in quota_container["models"]:
                        if not isinstance(model_item, dict):
                            continue
                        wkey = str(
                            model_item.get("name")
                            or model_item.get("model")
                            or model_item.get("id")
                            or "model"
                        )
                        rec = _extract_window_from_mapping(
                            peer, acct, wkey, model_item, source="cockpit_json", observed_at=obs
                        )
                        if rec:
                            records.append(rec)
                for wkey in ("five_hour", "seven_day", "hourly", "weekly", "primary", "secondary"):
                    sub = quota_container.get(wkey)
                    if isinstance(sub, dict):
                        rec = _extract_window_from_mapping(
                            peer, acct, wkey, sub, source="cockpit_json", observed_at=obs
                        )
                        if rec:
                            records.append(rec)
            for wkey in ("five_hour", "seven_day"):
                sub = entry.get(wkey)
                if isinstance(sub, dict):
                    rec = _extract_window_from_mapping(
                        peer, acct, wkey, sub, source="cockpit_json", observed_at=obs
                    )
                    if rec:
                        records.append(rec)
    return records


def _find_rate_limits_in_obj(obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return None
    if isinstance(obj.get("rate_limits"), dict):
        return obj["rate_limits"]
    payload = obj.get("payload")
    if isinstance(payload, dict):
        if isinstance(payload.get("rate_limits"), dict):
            return payload["rate_limits"]
        info = payload.get("info")
        if isinstance(info, dict) and isinstance(info.get("rate_limits"), dict):
            return info["rate_limits"]
    info = obj.get("info")
    if isinstance(info, dict) and isinstance(info.get("rate_limits"), dict):
        return info["rate_limits"]
    return None


def _format_window_minutes(minutes: Any, fallback_key: str, fallback_label: str) -> Tuple[str, str]:
    """Derive a compact window key and label from window_minutes when provided."""
    if minutes is None or isinstance(minutes, bool):
        return fallback_key, fallback_label
    try:
        mins = int(float(minutes))
    except (TypeError, ValueError):
        return fallback_key, fallback_label
    if mins <= 0:
        return fallback_key, fallback_label
    if mins % 1440 == 0:
        days = mins // 1440
        return f"{days}d", f"{days}d ({mins}m)"
    if mins % 60 == 0:
        hours = mins // 60
        return f"{hours}h", f"{hours}h ({mins}m)"
    return f"{mins}m", f"{mins}m rolling"


def _collect_codex_rollouts(codex_dir: str) -> List[Dict[str, Any]]:
    """Extract numeric rate_limits fields from Codex session logs without retaining raw lines."""
    sessions_dir = os.path.join(codex_dir, "sessions")
    if not os.path.isdir(sessions_dir):
        return []

    candidates: List[Tuple[float, str]] = []
    try:
        for root, _, files in os.walk(sessions_dir):
            for fname in files:
                if fname.startswith("rollout-") and fname.endswith(".jsonl"):
                    fpath = os.path.join(root, fname)
                    try:
                        candidates.append((os.path.getmtime(fpath), fpath))
                    except OSError:
                        pass
    except OSError:
        return []

    if not candidates:
        return []

    candidates.sort(reverse=True)
    for mtime, newest_path in candidates[:5]:
        try:
            with open(newest_path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - 131072), os.SEEK_SET)
                tail = handle.read().decode("utf-8", errors="replace")
        except OSError:
            continue

        for raw_line in reversed(tail.splitlines()):
            line = raw_line.strip()
            if not line or '"rate_limits"' not in line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            rate_limits = _find_rate_limits_in_obj(obj)
            if not rate_limits:
                continue

            obs = normalize_ts_to_iso(obj.get("timestamp")) or normalize_ts_to_iso(mtime)
            # Do not treat limit_id as an account identifier (R005 P2.5).
            acct = "default"
            records: List[Dict[str, Any]] = []
            for slot, default_wkey, default_label in (
                ("primary", "5h", "5h rolling"),
                ("secondary", "7d", "7d weekly"),
            ):
                slot_data = rate_limits.get(slot)
                if not isinstance(slot_data, dict):
                    continue
                pct = normalize_remaining_pct(
                    remaining=slot_data.get("remaining_percent"),
                    used=slot_data.get("used_percent"),
                )
                if pct is None:
                    continue
                wkey, label = _format_window_minutes(
                    slot_data.get("window_minutes"), default_wkey, default_label
                )
                records.append(
                    {
                        "peer_id": "chatgpt",
                        "account_id": acct,
                        "window_key": wkey,
                        "label": label,
                        "remaining_percent": pct,
                        "reset_at": normalize_ts_to_iso(slot_data.get("resets_at")),
                        "source": "codex_rollout",
                        "observed_at": obs,
                    }
                )
            if records:
                return records
    return []


def _collect_claude_local(claude_dir: str) -> List[Dict[str, Any]]:
    """Read local Claude usage/ratelimit cache files if present (zero token cost)."""
    records: List[Dict[str, Any]] = []
    for fname in ("usage-latest.json", "quota.json"):
        path = os.path.join(claude_dir, fname)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, ValueError):
            continue
        if not isinstance(raw, dict):
            continue

        acct = str(raw.get("account_id") or raw.get("email") or "default")
        obs = normalize_ts_to_iso(raw.get("observed_at") or raw.get("fetched_at"))

        for key, wkey, label in (
            ("five_hour", "5h", "5h rolling"),
            ("seven_day", "7d", "7d weekly"),
        ):
            sub = raw.get(key)
            if isinstance(sub, dict):
                rec = _extract_window_from_mapping(
                    "claude", acct, wkey, sub, source="claude_local", observed_at=obs
                )
                if rec:
                    rec["label"] = label
                    records.append(rec)

        if isinstance(raw.get("windows"), list):
            for item in raw["windows"]:
                if not isinstance(item, dict):
                    continue
                wkey = str(item.get("window_key") or item.get("key") or "window")
                rec = _extract_window_from_mapping(
                    "claude", acct, wkey, item, source="claude_local", observed_at=obs
                )
                if rec:
                    records.append(rec)

        headers = raw.get("headers") if isinstance(raw.get("headers"), dict) else raw
        for hdr_prefix, wkey, label in (
            ("anthropic-ratelimit-unified-5h", "5h", "5h rolling"),
            ("anthropic-ratelimit-unified-7d", "7d", "7d weekly"),
        ):
            rem_raw = headers.get(f"{hdr_prefix}-remaining")
            if rem_raw is not None:
                try:
                    num = float(rem_raw)
                except (TypeError, ValueError):
                    num = -1.0
                if num >= 0.0:
                    pct = normalize_remaining_pct(remaining=num, is_fraction=(num <= 1.0))
                    if pct is not None:
                        records.append(
                            {
                                "peer_id": "claude",
                                "account_id": acct,
                                "window_key": wkey,
                                "label": label,
                                "remaining_percent": pct,
                                "reset_at": normalize_ts_to_iso(
                                    headers.get(f"{hdr_prefix}-reset")
                                ),
                                "source": "claude_headers",
                                "observed_at": obs,
                            }
                        )
    return records


def sync_local_quotas(
    store: BusStore,
    user_home: Optional[str] = None,
) -> Dict[str, Any]:
    """Best-effort harvest of local quota & reset windows into BusStore."""
    if os.environ.get("DESKBELL_DISABLE_LOCAL_QUOTA_SYNC") == "1" and user_home is None:
        return store.list_quota_windows()
    effective_home = user_home or os.environ.get("DESKBELL_USER_HOME") or "~"
    home = os.path.abspath(os.path.expanduser(effective_home))
    discovered: List[Dict[str, Any]] = []
    discovered.extend(_collect_cockpit_sqlite(os.path.join(home, ".antigravity_cockpit")))
    discovered.extend(_collect_cockpit_json(os.path.join(home, ".antigravity_cockpit")))
    discovered.extend(_collect_codex_rollouts(os.path.join(home, ".codex")))
    discovered.extend(_collect_claude_local(os.path.join(home, ".claude")))

    if discovered:
        store.upsert_quota_windows(discovered)
    return store.list_quota_windows()
