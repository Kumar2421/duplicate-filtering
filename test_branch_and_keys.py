import argparse
import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Set

import httpx


DEFAULT_UPSTREAM_BRANCHES_URL = "https://api.analytics.thefusionapps.com/api/branch/branches"
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "backend", "config.json")
DEFAULT_LOGIN_URL = "https://auth.analytics.thefusionapps.com/api/auth/login"
DEFAULT_ORIGIN = "https://analytics.develop.thefusionapps.com"
DEFAULT_REFERER = "https://analytics.develop.thefusionapps.com/"
DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9,en-IN;q=0.8"


def _load_config_api_keys(config_path: str) -> Dict[str, str]:
    with open(config_path, "r") as f:
        cfg = json.load(f)

    api_cfgs = (cfg.get("api") or {}).get("configs") or []
    out: Dict[str, str] = {}
    for item in api_cfgs:
        branch_id = item.get("branchId")
        api_key = item.get("api_key")
        if branch_id and api_key:
            out[str(branch_id)] = str(api_key)
    return out


async def _fetch_upstream_branches(
    url: str,
    token: Optional[str],
    cache_bypass: bool,
    timeout_seconds: float,
    origin: Optional[str],
    referer: Optional[str],
    accept_language: Optional[str],
) -> List[str]:
    headers: Dict[str, str] = {}
    if cache_bypass:
        headers["x-cache-bypass"] = "true"
    headers["accept"] = "application/json, text/plain, */*"
    if accept_language:
        headers["accept-language"] = accept_language
    if origin:
        headers["origin"] = origin
    if referer:
        headers["referer"] = referer
    if token:
        headers["Authorization"] = f"Bearer {token}"

    timeout = httpx.Timeout(timeout_seconds, connect=min(10.0, timeout_seconds))
    async with httpx.AsyncClient(verify=False, timeout=timeout, http2=True) as client:
        res = await client.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()

    branches: List[str] = []
    if isinstance(data, dict):
        raw = data.get("branches")
        if isinstance(raw, list):
            branches = [str(b) for b in raw]
        else:
            raw2 = data.get("data")
            if isinstance(raw2, list):
                branches = [str(b) for b in raw2]
    elif isinstance(data, list):
        branches = [str(b) for b in data]

    return sorted(set(branches))


def _mask_token(token: str) -> str:
    t = token.strip()
    if len(t) <= 16:
        return "*" * len(t)
    return f"{t[:8]}...{t[-8:]}"


def _get_json_path(obj: Any, path: str) -> Optional[Any]:
    cur: Any = obj
    for part in path.split("."):
        if not part:
            return None
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


async def _login_for_token(
    *,
    login_url: str,
    email: str,
    password: str,
    device_id: Optional[str],
    cache_bypass: bool,
    timeout_seconds: float,
    token_json_path: str,
    origin: Optional[str],
    referer: Optional[str],
    accept_language: Optional[str],
) -> str:
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "accept": "application/json, text/plain, */*",
    }
    if cache_bypass:
        headers["x-cache-bypass"] = "true"
    if accept_language:
        headers["accept-language"] = accept_language
    if origin:
        headers["origin"] = origin
    if referer:
        headers["referer"] = referer

    payload: Dict[str, Any] = {
        "email": email,
        "password": password,
    }
    if device_id:
        payload["device_id"] = device_id

    timeout = httpx.Timeout(timeout_seconds, connect=min(10.0, timeout_seconds))
    async with httpx.AsyncClient(verify=False, timeout=timeout, http2=True) as client:
        res = await client.post(login_url, headers=headers, json=payload)
        res.raise_for_status()
        data = res.json()

    token_val = _get_json_path(data, token_json_path)
    if not token_val:
        raise RuntimeError(
            f"Login succeeded but token not found at json path '{token_json_path}'. "
            f"Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}"
        )
    return str(token_val)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--url", default=DEFAULT_UPSTREAM_BRANCHES_URL)
    parser.add_argument("--login-url", default=os.getenv("ANALYTICS_LOGIN_URL") or DEFAULT_LOGIN_URL)
    parser.add_argument("--origin", default=os.getenv("ANALYTICS_ORIGIN") or DEFAULT_ORIGIN)
    parser.add_argument("--referer", default=os.getenv("ANALYTICS_REFERER") or DEFAULT_REFERER)
    parser.add_argument("--accept-language", default=os.getenv("ANALYTICS_ACCEPT_LANGUAGE") or DEFAULT_ACCEPT_LANGUAGE)
    parser.add_argument("--email", default=os.getenv("ANALYTICS_EMAIL"))
    parser.add_argument("--password", default=os.getenv("ANALYTICS_PASSWORD"))
    parser.add_argument("--device-id", default=os.getenv("ANALYTICS_DEVICE_ID"))
    parser.add_argument("--token-json-path", default=os.getenv("ANALYTICS_TOKEN_JSON_PATH") or "token")
    parser.add_argument("--selected-branch", default=None)
    parser.add_argument("--token", default=os.getenv("ANALYTICS_TOKEN"))
    parser.add_argument("--login", action="store_true", default=False)
    parser.add_argument("--cache-bypass", action="store_true", default=True)
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()

    config_keys = _load_config_api_keys(args.config)
    config_branches: Set[str] = set(config_keys.keys())

    token_source = "env/arg"
    upstream_branches: List[str] = []
    upstream_error: Optional[str] = None

    if args.login:
        if not args.login_url:
            print("ERROR: --login was provided but --login-url is missing (or ANALYTICS_LOGIN_URL is not set).")
            return 2
        if not args.email or not args.password:
            print("ERROR: --login was provided but --email/--password are missing (or ANALYTICS_EMAIL/ANALYTICS_PASSWORD are not set).")
            return 2
        try:
            args.token = await _login_for_token(
                login_url=str(args.login_url),
                email=str(args.email),
                password=str(args.password),
                device_id=str(args.device_id) if args.device_id else None,
                cache_bypass=bool(args.cache_bypass),
                timeout_seconds=float(args.timeout),
                token_json_path=str(args.token_json_path),
                origin=str(args.origin) if args.origin else None,
                referer=str(args.referer) if args.referer else None,
                accept_language=str(args.accept_language) if args.accept_language else None,
            )
            token_source = "login"
        except Exception as e:
            print("=== Login ===")
            print(f"status: error")
            print(f"error: {e}")
            return 2
    try:
        upstream_branches = await _fetch_upstream_branches(
            url=args.url,
            token=args.token,
            cache_bypass=bool(args.cache_bypass),
            timeout_seconds=float(args.timeout),
            origin=str(args.origin) if args.origin else None,
            referer=str(args.referer) if args.referer else None,
            accept_language=str(args.accept_language) if args.accept_language else None,
        )
    except Exception as e:
        upstream_error = str(e)

    print("=== Upstream branch fetch ===")
    print(f"url: {args.url}")
    print(f"token_source: {token_source}")
    print(f"x-cache-bypass: {'true' if args.cache_bypass else 'false'}")
    print(f"token: {_mask_token(args.token) if args.token else '(none)'}")

    if upstream_error:
        print(f"status: error")
        print(f"error: {upstream_error}")
    else:
        print("status: ok")
        print(f"branches_count: {len(upstream_branches)}")
        if upstream_branches:
            print("branches:")
            for b in upstream_branches:
                print(f"- {b}")

    print("\n=== Config branch keys (backend/config.json) ===")
    print(f"config: {args.config}")
    print(f"configured_branches_count: {len(config_keys)}")
    if config_keys:
        print("configured_branches:")
        for b in sorted(config_keys.keys()):
            print(f"- {b}: {_mask_token(config_keys[b])}")

    print("\n=== Combined report ===")
    upstream_set: Set[str] = set(upstream_branches)
    if upstream_branches:
        missing_keys = sorted([b for b in upstream_set if b not in config_branches])
        extra_keys = sorted([b for b in config_branches if b not in upstream_set])

        print(f"upstream_without_config_key_count: {len(missing_keys)}")
        if missing_keys:
            print("upstream_without_config_key:")
            for b in missing_keys:
                print(f"- {b}")

        print(f"config_key_without_upstream_count: {len(extra_keys)}")
        if extra_keys:
            print("config_key_without_upstream:")
            for b in extra_keys:
                print(f"- {b}")

    if args.selected_branch:
        sb = str(args.selected_branch)
        print("\n=== Selected branch check ===")
        print(f"selectedBranch: {sb}")
        in_upstream = sb in upstream_set if upstream_branches else None
        has_key = sb in config_keys
        if in_upstream is None:
            print("in_upstream: (unknown; upstream fetch failed)")
        else:
            print(f"in_upstream: {str(in_upstream).lower()}")
        print(f"has_config_key: {str(has_key).lower()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
