import os

from fraud.common import load_dotenv


def test_reads_key_value_lines(tmp_path, monkeypatch):
    monkeypatch.delenv("PIPE_TEST_A", raising=False)
    monkeypatch.delenv("PIPE_TEST_B", raising=False)
    f = tmp_path / ".env"
    f.write_text('# a comment\n\nPIPE_TEST_A=one\nPIPE_TEST_B = "two words"\nnot a setting\n')
    load_dotenv(f)
    assert os.environ["PIPE_TEST_A"] == "one"
    assert os.environ["PIPE_TEST_B"] == "two words"  # spaces around = and surrounding quotes are dropped


def test_real_environment_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPE_TEST_C", "from the shell")
    f = tmp_path / ".env"
    f.write_text("PIPE_TEST_C=from the file\n")
    load_dotenv(f)
    assert os.environ["PIPE_TEST_C"] == "from the shell"


def test_missing_file_is_fine(tmp_path):
    load_dotenv(tmp_path / "nope.env")  # must not raise


def test_value_may_contain_an_equals_sign(tmp_path, monkeypatch):
    monkeypatch.delenv("PIPE_TEST_D", raising=False)
    f = tmp_path / ".env"
    f.write_text("PIPE_TEST_D=abc=def==\n")
    load_dotenv(f)
    assert os.environ["PIPE_TEST_D"] == "abc=def=="
