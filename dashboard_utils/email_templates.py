"""Email templates for outreach and follow-ups."""

# Google Apps Script web app URL for email open tracking.
# Fill this in after deploying EmailTracker.gs as a web app.
# Example: https://script.google.com/macros/s/XXXXXXXXX/exec
TRACKING_PIXEL_URL = "https://script.google.com/macros/s/AKfycbwByqYEHlIt9RPksowe1qWYH-iGWBk3Le1g2gMT_NuiNQE_BX-WVGDYG0iH4kEilgoV/exec"

OUTREACH_SUBJECT = "Paid Collaboration with Jobright.ai"

# Per-POC subject line overrides (default is OUTREACH_SUBJECT)
POC_SUBJECT_OVERRIDES = {
    "Jialin": "\U0001f4cc Paid Collaboration with Jobright.ai",
}


def get_subject_for_poc(poc_name: str) -> str:
    """Return the outreach subject line for a given POC."""
    return POC_SUBJECT_OVERRIDES.get(poc_name, OUTREACH_SUBJECT)

OUTREACH_BODY = """\
Hi {name},

Hope all is well! Loved your IG content and the tips you share in your posts. I wanted to reach out to see if you might be open to paid partnerships.

Jobright.ai helps people spend 90% less time on job search and land jobs in top companies, with over 2M+ users and access to 8M+ job listings. There's a great synergy with the content you're already sharing around careers, productivity tools, and AI.

Would love to explore if there's an opportunity to collaborate - we are open to long-term partnership. Let me know if you'd be interested, and feel free to send over your media kit and rate!

Best,
{sender_name}
Jobright.ai"""

FOLLOWUP_1_BODY = """\
Hi {name},

Just following up on my previous message - wanted to see if you'd be open to exploring a paid collaboration with Jobright.ai?

We're the most popular AI job search platform in the U.S. and Canada right now with 2M+ users, and we'd love to work with you. Happy to share more details if you're interested!

Best,
{sender_name}"""

FOLLOWUP_2_BODY = """\
Hi {name},

Just wanted to circle back one last time! We're actively partnering with creators in the career and AI space, and I think there's a really natural fit with your content.

If the timing isn't right, totally understand - but if you're open to it, I'd love to send over a quick overview of what a collaboration would look like. No pressure at all!

Best,
{sender_name}"""


PAYMENT_CONFIRM_SUBJECT = "Payment Confirmation — Jobright x {name}"

PAYMENT_CONFIRM_BODY = """\
Hi {name},

This is to confirm that your payment of ${amount} for the Jobright collaboration has been processed. Please allow 3-5 business days for the funds to appear in your account.

If you have any questions about your payment, feel free to reach out.

Thank you for being a part of the Jobright creator community!

Best,
{sender_name}
Jobright.ai"""


def format_email(template: str, name: str, sender_name: str) -> str:
    """Replace template variables."""
    return template.format(name=name, sender_name=sender_name)


def format_payment_email(template: str, name: str, sender_name: str, amount: str) -> str:
    """Replace template variables for payment confirmation."""
    return template.format(name=name, sender_name=sender_name, amount=amount)
