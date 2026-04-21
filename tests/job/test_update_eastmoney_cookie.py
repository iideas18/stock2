from instock.job import update_eastmoney_cookie as cli
from instock.core.eastmoney_cookie_manager import AcquisitionResult


def test_parse_args_rejects_show_cookie_with_env_mode():
    try:
        cli.parse_args(["--write", "env", "--show-cookie"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected parser to reject invalid combination")


def test_main_outputs_export_to_stdout_only_for_env(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))

    exit_code = cli.main(["--write", "env"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == cli.cookie_store.build_env_export("foo=bar")
    assert "foo=bar" not in out.err


def test_main_keeps_stdout_empty_on_non_zero_exit(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=4, cookie=None, validation_reason="bad-cookie"))

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 4
    assert out.out == ""


def test_main_outputs_full_cookie_only_after_successful_file_write(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file", "--show-cookie"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == "foo=bar\n"


def test_main_write_both_outputs_export_even_when_file_is_unchanged(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setenv("EAST_MONEY_COOKIE", "foo=bar")
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": False, "warning": None})())

    exit_code = cli.main(["--write", "both"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert out.out == cli.cookie_store.build_env_export("foo=bar")
    assert out.err.count("unchanged") >= 1


def test_main_maps_write_failure_to_exit_code_5(monkeypatch, capsys):
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))

    exit_code = cli.main(["--write", "file", "--show-cookie"])
    out = capsys.readouterr()

    assert exit_code == 5
    assert out.out == ""
    assert "WARNING:" in out.err


def test_main_logs_observed_status_cookie_names(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "acquire_cookie",
        lambda args: AcquisitionResult(
            exit_code=0,
            cookie="foo=bar",
            validation_reason="ok",
            observed_status_cookie_names=("st_pvi", "st_si"),
        ),
    )
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert "st_pvi" in out.err and "st_si" in out.err


def test_main_prints_mode_specific_env_warning_for_write_file(monkeypatch, capsys):
    monkeypatch.setenv("EAST_MONEY_COOKIE", "env=1")
    monkeypatch.setattr(cli, "acquire_cookie", lambda args: AcquisitionResult(exit_code=0, cookie="foo=bar", validation_reason="ok"))
    monkeypatch.setattr(cli.cookie_store, "write_cookie_file", lambda *_args, **_kwargs: type("R", (), {"changed": True, "warning": None})())

    exit_code = cli.main(["--write", "file"])
    out = capsys.readouterr()

    assert exit_code == 0
    assert "still prefers the environment value instead of the file" in out.err


def test_main_passes_parsed_browser_and_timeout_to_acquire(monkeypatch):
    captured = {}

    def fake_acquire(args):
        captured["browser"] = args.browser
        captured["timeout"] = args.timeout
        return AcquisitionResult(exit_code=3, cookie=None, validation_reason="timeout")

    monkeypatch.setattr(cli, "acquire_cookie", fake_acquire)

    exit_code = cli.main(["--write", "file", "--browser", "msedge", "--timeout", "42"])

    assert exit_code == 3
    assert captured == {"browser": "msedge", "timeout": 42}
