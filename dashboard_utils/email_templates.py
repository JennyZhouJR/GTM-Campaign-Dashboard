"""Email templates for outreach and follow-ups."""

OUTREACH_SUBJECT = "Paid Collaboration with Jobright.ai"

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


def format_email(template: str, name: str, sender_name: str) -> str:
    """Replace template variables."""
    return template.format(name=name, sender_name=sender_name)
