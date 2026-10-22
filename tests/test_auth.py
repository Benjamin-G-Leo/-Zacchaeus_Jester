import pytest

from fraud.auth import failure_delay, password_ok


def test_right_password_passes():
    assert password_ok("correct horse", "correct horse")


@pytest.mark.parametrize("entered", ["", "wrong", "correct horse ", "CORRECT HORSE", None, 5, b"correct horse"])
def test_wrong_or_odd_input_fails(entered):
    assert not password_ok(entered, "correct horse")


@pytest.mark.parametrize("expected", ["", None])
def test_no_password_set_never_matches(expected):
    """An unset password means "no login", so it must not accept an empty try as correct."""
    assert not password_ok("", expected)
    assert not password_ok("anything", expected)


def test_non_ascii_passwords_work():
    assert password_ok("pässwörd-密码", "pässwörd-密码")
    assert not password_ok("passwörd-密码", "pässwörd-密码")


def test_delay_grows_then_caps():
    assert [failure_delay(n) for n in range(0, 8)] == [0, 1, 2, 4, 8, 16, 30, 30]
