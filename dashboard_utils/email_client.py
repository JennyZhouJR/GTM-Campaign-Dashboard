"""Gmail SMTP/IMAP client for sending outreach emails and checking replies."""

import smtplib
import imaplib
import email as email_lib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid, formatdate
from datetime import datetime, timedelta
import time

from urllib.parse import quote

from dashboard_utils.email_templates import (
    OUTREACH_SUBJECT, OUTREACH_BODY, FOLLOWUP_1_BODY, FOLLOWUP_2_BODY,
    PAYMENT_CONFIRM_SUBJECT, PAYMENT_CONFIRM_BODY,
    TRACKING_PIXEL_URL,
    format_email, format_payment_email, get_subject_for_poc,
)


def _wrap_html(body_text: str, message_id: str) -> str:
    """Wrap plain text body in HTML with tracking pixel at end."""
    # Convert plain text to HTML (preserve line breaks, escape minimal)
    html_body = body_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_body = html_body.replace("\n", "<br>\n")
    # Strip message-ID brackets for use in URL
    clean_id = message_id.strip("<>")
    pixel = ""
    if TRACKING_PIXEL_URL:
        pixel_url = f"{TRACKING_PIXEL_URL}?id={quote(clean_id)}"
        pixel = f'<img src="{pixel_url}" width="1" height="1" style="display:none" alt="">'
    return (
        f'<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#222;">'
        f'{html_body}'
        f'{pixel}'
        f'</body></html>'
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

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg_id = make_msgid(domain=sender_email.split("@")[1])
    msg["Message-ID"] = msg_id
    msg["Date"] = formatdate(localtime=True)
    # Plain text (fallback)
    msg.attach(MIMEText(body, "plain"))
    # HTML with tracking pixel
    msg.attach(MIMEText(_wrap_html(body, msg_id), "html"))

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

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    new_msg_id = make_msgid(domain=sender_email.split("@")[1])
    msg["Message-ID"] = new_msg_id
    msg["In-Reply-To"] = original_msg_id
    msg["References"] = original_msg_id
    msg["Date"] = formatdate(localtime=True)
    # Plain text (fallback)
    msg.attach(MIMEText(body, "plain"))
    # HTML with tracking pixel
    msg.attach(MIMEText(_wrap_html(body, new_msg_id), "html"))

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


# Tri-state reply check results
REPLY_YES = "replied"       # Confirmed they replied
REPLY_NO = "no_reply"       # Confirmed they did NOT reply
REPLY_UNKNOWN = "unknown"   # Could not verify (IMAP/auth/network error)


def check_reply_status(sender_email: str, app_password: str, original_msg_id: str) -> str:
    """Check via IMAP if the thread has been continued. Returns REPLY_YES / REPLY_NO / REPLY_UNKNOWN.

    Returns REPLY_YES if the email thread has been continued by ANY party —
    either the recipient replied (in INBOX) OR the sender manually followed up
    via Gmail (in SENT). In both cases, auto_followup should skip to avoid
    duplicate emails.

    REPLY_UNKNOWN is returned on any error (auth failure, timeout, IMAP down) —
    callers should treat this as "don't send follow-up" (fail-closed).

    Searches "[Gmail]/All Mail" which includes INBOX + SENT + all labels.

    Uses Gmail's X-GM-RAW extension with the `rfc822msgid:` operator, which
    queries Gmail's pre-indexed Message-ID field. This is 10-100x faster than
    a standard IMAP HEADER scan (which crawls every message body) and avoids
    read timeouts for users with large mailboxes.
    """
    mail = None
    try:
        # 30s timeout (was 15s) — HEADER search on large mailboxes could time out.
        # With X-GM-RAW + rfc822msgid: we're now fast, but keep margin.
        mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=30)
        mail.login(sender_email, app_password)
        # [Gmail]/All Mail is a virtual folder containing INBOX + SENT + everything.
        # This lets us detect both incoming replies AND the user's own manual
        # follow-ups (which live in SENT, not INBOX).
        _status, _ = mail.select('"[Gmail]/All Mail"')
        if _status != "OK":
            # Fallback if All Mail isn't available (e.g. non-Gmail or renamed label)
            mail.select("INBOX")

        # Strip angle brackets — rfc822msgid: operator wants bare ID
        clean_id = original_msg_id.strip().strip("<>")
        # Escape any embedded quotes defensively (make_msgid doesn't produce them)
        safe_id = clean_id.replace('"', '\\"')

        # Gmail's X-GM-RAW extension uses Gmail's search syntax (indexed, fast).
        # `rfc822msgid:<id>` matches the original Message-ID; Gmail automatically
        # includes messages whose In-Reply-To or References header points at it
        # (same thread), so one query covers both.
        typ, data = mail.search(None, "X-GM-RAW", f'"rfc822msgid:{safe_id}"')
        if typ == "OK" and data and data[0]:
            # The search matches the ORIGINAL message too (thread grouping).
            # We need to know: are there OTHER messages in this thread besides the original?
            ids = data[0].split()
            if len(ids) > 1:
                return REPLY_YES

        # Fallback: standard IMAP HEADER search (old path, in case X-GM-RAW behaves
        # differently on a specific account)
        _, data = mail.search(None, f'(HEADER In-Reply-To "{safe_id}")')
        if data and data[0]:
            return REPLY_YES
        _, data = mail.search(None, f'(HEADER References "{safe_id}")')
        if data and data[0]:
            return REPLY_YES

        return REPLY_NO
    except Exception as e:
        # Log but don't raise — caller decides what to do with UNKNOWN
        print(f"⚠️ IMAP check_reply failed for {sender_email}: {e}")
        return REPLY_UNKNOWN
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:
                pass


def check_reply(sender_email: str, app_password: str, original_msg_id: str) -> bool:
    """Legacy wrapper — returns True ONLY if we confirmed a reply.

    Callers wanting the tri-state (to skip sends on UNKNOWN) should use
    check_reply_status directly.
    """
    return check_reply_status(sender_email, app_password, original_msg_id) == REPLY_YES


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
