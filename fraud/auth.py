"""Tiny helpers for the dashboard's shared password."""
import hmac


def password_ok(entered, expected):
    """True only if `entered` equals `expected`.

    Compared in constant time, so response timing does not leak how many characters matched.
    An empty or missing expected password never matches (that case means "no password set").
    """
    if not expected or not isinstance(entered, str):
        return False
    return hmac.compare_digest(entered.encode(), expected.encode())


def failure_delay(failures):
    """Seconds to wait after this many wrong tries in a row: 1, 2, 4, ... capped at 30."""
    return 0 if failures <= 0 else min(2 ** (failures - 1), 30)
