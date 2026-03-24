import contextlib
from types import SimpleNamespace

from kink import di

import main as main_module
from program.program import Program, ProgramRuntimeError


class DummyProgram:
    def __init__(self, *, wait_error: Exception | None = None):
        self.wait_error = wait_error
        self.calls = []

    def start(self) -> None:
        self.calls.append("start")

    def wait_until_stopped(self) -> None:
        self.calls.append("wait")
        if self.wait_error is not None:
            raise self.wait_error

    def stop(self) -> None:
        self.calls.append("stop")


class FakeServer:
    def __init__(self, config):
        self.config = config

    @contextlib.contextmanager
    def run_in_thread(self):
        yield


def _configure_main_dependencies(monkeypatch, program: DummyProgram) -> object:
    previous_program = di[Program]
    di[Program] = program
    monkeypatch.setattr(main_module, "Server", FakeServer)
    monkeypatch.setattr(main_module.uvicorn, "Config", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        main_module,
        "handle_args",
        lambda: SimpleNamespace(port=8080),
    )
    monkeypatch.setattr(main_module.signal, "signal", lambda *args, **kwargs: None)
    return previous_program


def test_main_waits_for_program_thread(monkeypatch):
    program = DummyProgram()
    previous_program = _configure_main_dependencies(monkeypatch, program)

    try:
        exit_code = main_module.main()
    finally:
        di[Program] = previous_program

    assert exit_code == main_module.EXIT_OK
    assert program.calls == ["start", "wait", "stop"]


def test_main_returns_runtime_error_when_program_thread_fails(monkeypatch):
    program = DummyProgram(wait_error=ProgramRuntimeError("boom"))
    previous_program = _configure_main_dependencies(monkeypatch, program)

    try:
        exit_code = main_module.main()
    finally:
        di[Program] = previous_program

    assert exit_code == main_module.EXIT_RUNTIME_ERROR
    assert program.calls == ["start", "wait", "stop"]
