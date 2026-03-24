from collections.abc import Awaitable, Callable
import contextlib
import signal
import sys
import threading
import time
from types import FrameType

from kink import di
import uvicorn
from dotenv import load_dotenv

load_dotenv()  # import required here to support SETTINGS_FILENAME

from program.utils.proxy_client import ProxyClient
from program.utils.async_client import AsyncClient

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from scalar_fastapi import (
    get_scalar_api_reference,  # pyright: ignore[reportUnknownVariableType]
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from program.program import Program, ProgramRuntimeError, ProgramStartupError, riven
from program.settings.models import get_version
from program.settings import settings_manager
from program.utils.cli import handle_args
from routers import app_router


class LoguruMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        start_time = time.time()
        response = None

        try:
            response = await call_next(request)

            return response
        except Exception as e:
            logger.exception(f"Exception during request processing: {e}")
            raise
        finally:
            process_time = time.time() - start_time

            logger.log(
                "API",
                f"{request.method} {request.url.path} - {response.status_code if response else '500'} - {process_time:.2f}s",
            )


EXIT_OK = 0
EXIT_RUNTIME_ERROR = 1


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    di[AsyncClient] = AsyncClient()

    proxy_url = settings_manager.settings.downloaders.proxy_url

    if proxy_url:
        di[ProxyClient] = ProxyClient(proxy_url=proxy_url)

    yield

    await di[AsyncClient].aclose()

    if ProxyClient in di:
        await di[ProxyClient].aclose()


app = FastAPI(
    title="Riven",
    summary="A media management system.",
    version=get_version(),
    redoc_url=None,
    license_info={
        "name": "GPL-3.0",
        "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
    },
    lifespan=lifespan,
)


@app.get("/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title=app.title,
    )


@app.get("/livez", include_in_schema=False)
async def livez() -> dict[str, object]:
    return {
        "status": "ok",
        "service": app.title,
        "version": app.version,
    }


@app.get("/readyz", include_in_schema=False)
async def readyz(response: Response) -> dict[str, object]:
    runtime_status = di[Program].get_runtime_status()

    if runtime_status["ready"]:
        return {
            "status": "ready",
            **runtime_status,
        }

    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "status": "not_ready",
        **runtime_status,
    }


di[Program] = riven

app.add_middleware(LoguruMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(app_router)


class Server(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run, name="RivenAPI", daemon=True)
        thread.start()

        try:
            while not self.started and thread.is_alive():
                time.sleep(1e-3)

            if not self.started:
                raise RuntimeError("Uvicorn server failed to start.")

            yield
        except Exception:
            logger.exception("Error in server thread")
            raise
        finally:
            self.should_exit = True
            thread.join(timeout=5)


def signal_handler(_signum: int, _frame: FrameType | None):
    logger.log("PROGRAM", "Exiting Gracefully.")
    di[Program].stop()
    raise SystemExit(EXIT_OK)


def main() -> int:
    args = handle_args()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_config=None)
    server = Server(config=config)
    exit_code = EXIT_OK

    try:
        with server.run_in_thread():
            di[Program].start()
            di[Program].wait_until_stopped()
    except SystemExit as exc:
        exit_code = int(exc.code) if isinstance(exc.code, int) else EXIT_OK
    except ProgramStartupError as exc:
        logger.error(f"Startup failed: {exc}")
        logger.exception("Error while starting Riven")
        exit_code = EXIT_RUNTIME_ERROR
    except ProgramRuntimeError as exc:
        logger.error(f"Runtime failed: {exc}")
        logger.exception("Error while running Riven")
        exit_code = EXIT_RUNTIME_ERROR
    except Exception:
        logger.exception("Error in main thread")
        exit_code = EXIT_RUNTIME_ERROR
    finally:
        di[Program].stop()
        logger.critical("Server has been stopped")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
