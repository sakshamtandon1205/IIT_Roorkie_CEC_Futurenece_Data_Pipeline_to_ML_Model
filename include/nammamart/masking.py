"""Masking for personal data. Reporting tables only ever get the masked value."""


def mask_email(value) -> str:
    """priya.hegde199@example.com -> pr***@example.com"""
    v = str(value).strip()
    if not v:
        return ""
    if "@" not in v:
        return mask_text(v)
    local, domain = v.split("@", 1)
    return local[:2] + "***@" + domain


def mask_phone(value) -> str:
    """9774349021 -> 97******21 (works for odd lengths too)"""
    v = str(value).strip()
    if not v:
        return ""
    if len(v) <= 4:
        return "*" * len(v)
    return v[:2] + "*" * (len(v) - 4) + v[-2:]


def mask_text(value) -> str:
    v = str(value).strip()
    return v[:1] + "*" * max(len(v) - 1, 0)


def mask_name(value) -> str:
    """Priya Hegde -> Priya H."""
    parts = str(value).split()
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return parts[0] + " " + parts[-1][0] + "."
