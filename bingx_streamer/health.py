import asyncio
import json
import logging
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING, Callable, Dict, Optional

import psutil

from .config import Config

if TYPE_CHECKING:
    from .client import BingXCompleteClient


log = logging.getLogger("bingx.health")


class SystemLoadMonitor:
    """Monitors system CPU and memory usage"""

    @staticmethod
    def get_load() -> Dict[str, float]:
        """Get current CPU and memory usage"""
        return {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent,
        }

    @staticmethod
    async def sleep_if_under_load(config: Config = Config()) -> None:
        """Sleep for a duration proportional to system load if it exceeds thresholds"""
        load: Dict[str, float] = SystemLoadMonitor.get_load()
        cpu: float = load["cpu_percent"]
        mem: float = load["memory_percent"]

        if cpu > config.CPU_THRESHOLD or mem > config.MEMORY_THRESHOLD:
            # Calculate sleep duration, more load = longer sleep
            cpu_factor: float = max(
                0, (cpu - config.CPU_THRESHOLD) / (100 - config.CPU_THRESHOLD)
            )
            mem_factor: float = max(
                0, (mem - config.MEMORY_THRESHOLD) / (100 - config.MEMORY_THRESHOLD)
            )
            load_factor: float = max(cpu_factor, mem_factor)

            sleep_duration: float = config.MAX_SLEEP_ON_LOAD * load_factor

            log.warning(
                f"High system load detected (CPU: {cpu:.1f}%, Mem: {mem:.1f}%). "
                f"Throttling for {sleep_duration:.3f}s."
            )
            await asyncio.sleep(sleep_duration)


class HealthCheckServer(threading.Thread):
    """Simple HTTP server for health checks"""

    def __init__(self, port: int, client: "BingXCompleteClient") -> None:
        super().__init__(daemon=True)
        self.port: int = port
        self.client: "BingXCompleteClient" = client
        self.server: Optional[HTTPServer] = None

    def run(self) -> None:
        handler = self._create_handler()
        self.server = HTTPServer(("", self.port), handler)
        log.info(f"Health check server started on http://localhost:{self.port}")
        self.server.serve_forever()

    def _create_handler(self) -> Callable[..., BaseHTTPRequestHandler]:
        client = self.client

        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health":
                    is_healthy, reason = client.is_healthy()
                    if is_healthy:
                        self.send_response(200)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"status": "ok"}).encode())
                    else:
                        self.send_response(503)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(
                            json.dumps(
                                {"status": "unhealthy", "reason": reason}
                            ).encode()
                        )
                else:
                    self.send_response(404)
                    self.end_headers()

        return HealthCheckHandler

    def shutdown(self) -> None:
        if self.server:
            log.info("Shutting down health check server...")
            self.server.shutdown()


def find_free_port() -> int:
    """Find an available port on the host"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
