import pytest

from instock.core import eastmoney_cookie_store as store


def test_normalize_file_text_strips_only_one_line_ending():
    assert store.normalize_file_text("a=b\r\n") == "a=b"
    assert store.normalize_file_text("a=b\n") == "a=b"
    assert store.normalize_file_text("a=b\r") == "a=b"
    assert store.normalize_file_text("a=b \n") == "a=b "
    assert store.normalize_file_text("a=b\n\n") == "a=b\n"


def test_build_env_export_uses_shell_safe_quoting():
    export_line = store.build_env_export("a=b'c")
    assert export_line.startswith("export EAST_MONEY_COOKIE=")
    assert export_line.endswith("\n")
    assert '"\'"\'' in export_line


def test_read_cookie_file_returns_none_when_missing(tmp_path):
    assert store.read_cookie_file(tmp_path / "missing.txt") is None


def test_mask_cookie_redacts_middle_content():
    assert store.mask_cookie("abcdefgh12345678") == "abcdefgh...len=16"


def test_write_cookie_file_returns_changed_false_when_content_is_unchanged(tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    path.write_text("foo=bar\n", encoding="utf-8")

    result = store.write_cookie_file("foo=bar", path)

    assert result.changed is False
    assert path.read_text(encoding="utf-8") == "foo=bar\n"


def test_write_cookie_file_rejects_control_characters(tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    with pytest.raises(ValueError):
        store.write_cookie_file("foo=bar\nboom", path)


def test_write_cookie_file_returns_warning_when_chmod_fails(monkeypatch, tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"

    def raise_chmod(*_args, **_kwargs):
        raise OSError("no chmod")

    monkeypatch.setattr(store.os, "chmod", raise_chmod)
    result = store.write_cookie_file("foo=bar", path)

    assert result.changed is True
    assert result.warning == "chmod-failed"


def test_write_cookie_file_cleans_up_temp_file_when_replace_fails(monkeypatch, tmp_path):
    path = tmp_path / "eastmoney_cookie.txt"
    created = {}
    original_replace = store.os.replace

    def tracking_replace(src, dst):
        created["tmp"] = src
        raise OSError("replace failed")

    monkeypatch.setattr(store.os, "replace", tracking_replace)

    with pytest.raises(OSError):
        store.write_cookie_file("foo=bar", path)

    assert created["tmp"].exists() is False


def test_write_cookie_file_propagates_mkdir_failure(monkeypatch, tmp_path):
    path = tmp_path / "nested" / "eastmoney_cookie.txt"

    def raise_mkdir(*_args, **_kwargs):
        raise OSError("mkdir failed")

    monkeypatch.setattr(store.Path, "mkdir", raise_mkdir)

    with pytest.raises(OSError):
        store.write_cookie_file("foo=bar", path)
