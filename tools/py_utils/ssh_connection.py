"""SSH helpers for local tools that inspect cluster services."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass


@dataclass
class SSHConfig:
    """SSH configuration for reaching cluster resources through a jump host."""

    jump_host: str
    cluster_host: str
    verbose: bool = False


class SSHConnection:
    """Run commands on a cluster host through the configured jump host."""

    def __init__(self, config: SSHConfig) -> None:
        self.config = config

    def run(
        self,
        cmd: str,
        retries: int = 3,
        delay_seconds: int = 2,
        timeout: int = 90,
    ) -> str:
        """Execute a remote command with light retry handling for transient errors."""
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                return self._run_once(cmd, timeout)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                msg = str(exc).lower()
                transient = (
                    "exit code: 28" in msg
                    or "timed out" in msg
                    or "failed to connect" in msg
                    or "connection refused" in msg
                )
                if not transient or attempt == retries:
                    raise
                if self.config.verbose:
                    print(f"[debug] transient error {attempt}/{retries}: {exc}")
                time.sleep(delay_seconds)
        if last_exc:
            raise last_exc
        raise RuntimeError("Unexpected retry flow.")

    def _run_once(self, cmd: str, timeout: int) -> str:
        """Execute one remote command attempt."""
        full_cmd = ["ssh", "-J", self.config.jump_host, self.config.cluster_host, cmd]
        if self.config.verbose:
            print(f"[debug] remote cmd: {cmd}")
        result = subprocess.run(
            full_cmd,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if self.config.verbose:
            if result.stdout.strip():
                print(f"[debug] stdout: {result.stdout.strip()}")
            if result.stderr.strip():
                print(f"[debug] stderr: {result.stderr.strip()}")
        if result.returncode != 0:
            raise RuntimeError(
                "Remote command failed.\n"
                f"Command: {cmd}\n"
                f"Exit code: {result.returncode}\n"
                f"STDOUT: {result.stdout}\n"
                f"STDERR: {result.stderr}"
            )
        return result.stdout.strip()
