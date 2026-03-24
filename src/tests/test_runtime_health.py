from fastapi.testclient import TestClient
from kink import di

from main import app
from program.program import Program


def test_livez_returns_ok():
    client = TestClient(app)

    response = client.get("/livez")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_readyz_returns_200_when_program_is_ready():
    class ReadyProgram:
        def get_runtime_status(self) -> dict[str, bool]:
            return {
                "ready": True,
                "initialized": True,
                "running": True,
                "database_ready": True,
                "services_initialized": True,
                "services_valid": True,
                "scheduler_running": True,
            }

    previous_program = di[Program]
    di[Program] = ReadyProgram()

    try:
        client = TestClient(app)
        response = client.get("/readyz")
    finally:
        di[Program] = previous_program

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_readyz_returns_503_when_program_is_not_ready():
    class NotReadyProgram:
        def get_runtime_status(self) -> dict[str, bool]:
            return {
                "ready": False,
                "initialized": True,
                "running": False,
                "database_ready": False,
                "services_initialized": True,
                "services_valid": False,
                "scheduler_running": False,
            }

    previous_program = di[Program]
    di[Program] = NotReadyProgram()

    try:
        client = TestClient(app)
        response = client.get("/readyz")
    finally:
        di[Program] = previous_program

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
