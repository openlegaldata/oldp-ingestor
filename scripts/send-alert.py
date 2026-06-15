#!/usr/bin/env python3
"""Send email alerts for OLDP ingestor issues.

Usage:
    send-alert.py --subject "..." --body "..."
    echo "body text" | send-alert.py --subject "..."

Reads SMTP config from environment variables (set in oldp-ingestor-cron.env).
"""

import argparse
import os
import smtplib
import sys
from email.mime.text import MIMEText


def send_alert(subject: str, body: str) -> bool:
    host = os.environ.get("ALERT_SMTP_HOST", "")
    port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
    user = os.environ.get("ALERT_SMTP_USER", "")
    password = os.environ.get("ALERT_SMTP_PASSWORD", "")
    use_tls = os.environ.get("ALERT_SMTP_TLS", "1") == "1"
    from_addr = os.environ.get("ALERT_FROM", "")
    to_addr = os.environ.get("ALERT_TO", "")

    if not all([host, user, password, from_addr, to_addr]):
        print("ERROR: Missing SMTP environment variables", file=sys.stderr)
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    try:
        if use_tls:
            server = smtplib.SMTP(host, port)
            server.starttls()
        else:
            server = smtplib.SMTP(host, port)
        server.login(user, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
        print(f"Alert sent to {to_addr}: {subject}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to send alert: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Send OLDP ingestor alert email")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--body", default=None, help="Email body (reads stdin if omitted)")
    args = parser.parse_args()

    if args.body:
        body = args.body
    elif not sys.stdin.isatty():
        body = sys.stdin.read()
    else:
        print("ERROR: Provide --body or pipe text to stdin", file=sys.stderr)
        sys.exit(1)

    success = send_alert(args.subject, body)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
