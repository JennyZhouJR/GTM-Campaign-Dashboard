"""Gmail SMTP/IMAP client for sending outreach emails and checking replies."""

import smtplib
import imaplib
import email as email_lib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid, formatdate
from datetime import datetime, timedelta
import time

from dashboard_utils.email_templates import (
    OUTREACH_SUBJECT, OUTREACH_BODY, FOLLOWUP_1_BODY, FOLLOWUP_2_BODY,
    PAYMENT_CONFIRM_SUBJECT, PAYMENT_CONFIRM_BODY,
    format_email, format_payment_email, get_subject_for_poc,
)


def test_smtp_connection(sender_email: str, app_password: str) -> bool:
    """Test SMTP login. Returns True if successful."""
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587, timeout=10)
        server.starttls()
        server.login(sender_email, app_password)
        server.quit()
        return True
    except Exception:
        return False


def _get_smtp(sender_email: str, app_password: str) -> smtplib.SMTP:
    """Create and return an authenticated SMTP connection."""
    server = smtplib.SMTP("smtp.gmail.com", 587, timeout=30)
    server.starttls()
    server.login(sender_email, app_password)
    return server


def send_outreach(
    sender_email: str,
    app_password: str,
    to_email: str,
    recipient_name: str,
    sender_name: str,
) -> str:
    """Send initial outreach email. Returns the Message-ID."""
    body = format_email(OUTREACH_BODY, name=recipient_name, sender_name=sender_name)
    subject = get_subject_for_poc(sender_name)

    msg = MIMEMultipart()
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg_id = make_msgid(domain=sender_email.split("@")[1])
    msg["Message-ID"] = msg_id
    msg["Date"] = formatdate(localtime=True)
    msg.attach(MIMEText(body, "plain"))

    server = _get_smtp(sender_email, app_password)
    server.sendmail(sender_email, to_email, msg.as_string())
    server.quit()

    return msg_id


def send_followup(
    sender_email: str,
    app_password: str,
    to_email: str,
    recipient_name: str,
    sender_name: str,
    original_msg_id: str,
    followup_num: int,
) -> str:
    """Send follow-up email in the same thread. Returns the new Message-ID."""
    if followup_num == 1:
        body = format_email(FOLLOWUP_1_BODY, name=recipient_name, sender_name=sender_name)
    else:
        body = format_email(FOLLOWUP_2_BODY, name=recipient_name, sender_name=sender_name)

    subject = f"Re: {get_subject_for_poc(sender_name)}"

    msg = MIMEMultipart()
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    new_msg_id = make_msgid(domain=sender_email.split("@")[1])
    msg["Message-ID"] = new_msg_id
    msg["In-Reply-To"] = original_msg_id
    msg["References"] = original_msg_id
    msg["Date"] = formatdate(localtime=True)
    msg.attach(MIMEText(body, "plain"))

    server = _get_smtp(sender_email, app_password)
    server.sendmail(sender_email, to_email, msg.as_string())
    server.quit()

    return new_msg_id


def send_payment_confirmation(
    sender_email: str,
    app_password: str,
    to_email: str,
    recipient_name: str,
    sender_name: str,
    amount: str,
) -> str:
    """Send payment confirmation email. Returns the Message-ID."""
    first_name = recipient_name.strip().split()[0] if recipient_name.strip() else "there"
    body = format_payment_email(
        PAYMENT_CONFIRM_BODY, name=first_name, sender_name=sender_name, amount=amount,
    )
    subject = PAYMENT_CONFIRM_SUBJECT.format(name=recipient_name.strip())

    msg = MIMEMultipart()
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg_id = make_msgid(domain=sender_email.split("@")[1])
    msg["Message-ID"] = msg_id
    msg["Date"] = formatdate(localtime=True)
    msg.attach(MIMEText(body, "plain"))

    server = _get_smtp(sender_email, app_password)
    server.sendmail(sender_email, to_email, msg.as_string())
    server.quit()

    return msg_id


def check_reply(sender_email: str, app_password: str, original_msg_id: str) -> bool:
    """Check via IMAP if someone replied to the email with the given Message-ID."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=15)
        mail.login(sender_email, app_password)
        mail.select("INBOX")

        # Search for emails that reference the original message
        clean_id = original_msg_id.strip("<>")
        _, data = mail.search(None, f'(HEADER In-Reply-To "{original_msg_id}")')
        if data[0]:
            mail.logout()
            return True

        # Also check References header
        _, data = mail.search(None, f'(HEADER References "{original_msg_id}")')
        if data[0]:
            mail.logout()
            return True

        mail.logout()
        return False
    except Exception:
        return False


def batch_send_outreach(
    sender_email: str,
    app_password: str,
    sender_name: str,
    recipients: list,
    progress_callback=None,
) -> list:
    """Send outreach to multiple recipients.

    Args:
        recipients: list of dicts with keys: to_email, name, sheet_row
        progress_callback: optional fn(current, total, name, success)

    Returns:
        list of dicts: {sheet_row, msg_id, success, error}
    """
    results = []
    total = len(recipients)

    for i, r in enumerate(recipients):
        try:
            msg_id = send_outreach(
                sender_email, app_password,
                r["to_email"], r["name"], sender_name,
            )
            results.append({
                "sheet_row": r["sheet_row"],
                "msg_id": msg_id,
                "success": True,
                "error": None,
            })
            if progress_callback:
                progress_callback(i + 1, total, r["name"], True)
        except Exception as e:
            results.append({
                "sheet_row": r["sheet_row"],
                "msg_id": None,
                "success": False,
                "error": str(e),
            })
            if progress_callback:
                progress_callback(i + 1, total, r["name"], False)

        # Small delay to avoid Gmail rate limits
        if i < total - 1:
            time.sleep(1)

    return results
