"""Controlled one-shot Telegram probe.

Usage:
  python3 app/common/tools/telegram_controlled_probe.py --message "text"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

APP_DIR = Path("/Users/miguelmiguel/CODEX/HREVN UNIFIED V1 SANDBOX/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from common.services.telegram_connector import send_controlled_test_message


def main() -> int:
    parser = argparse.ArgumentParser(description="Send one controlled Telegram test message.")
    parser.add_argument("--message", required=True, help="Message body to send")
    args = parser.parse_args()

    ok, detail = send_controlled_test_message(args.message)
    print(f"ok={ok} detail={detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
