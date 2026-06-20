from __future__ import annotations

from functools import cached_property
from typing import Iterable, Literal
from urllib3.exceptions import HTTPError

from kubernetes.client.exceptions import ApiException

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodLoggingStatus,
    PodManager,
)


_TRANSIENT_LOG_STATUS_CODES = {500, 502, 503, 504}


def _is_transient_log_exception(exc: ApiException) -> bool:
    """Return True for kubelet/containerLogs failures that should not fail Spark."""
    status = getattr(exc, "status", None)
    reason = str(getattr(exc, "reason", "") or "")
    body = str(getattr(exc, "body", "") or "")
    message = f"{reason} {body} {exc}"
    transient_markers = (
        "containerLogs",
        "TLS handshake timeout",
        "timed out",
        "Internal Server Error",
        "Service Unavailable",
        "Bad Gateway",
        "Gateway Timeout",
    )
    return status in _TRANSIENT_LOG_STATUS_CODES or any(
        marker in message for marker in transient_markers
    )


class SafeSparkPodManager(PodManager):
    """PodManager variant that treats transient driver log API failures as retryable."""

    def read_pod_logs(self, *args, **kwargs):
        try:
            return super().read_pod_logs(*args, **kwargs)
        except ApiException as exc:
            if _is_transient_log_exception(exc):
                self.log.warning(
                    "Spark driver log stream temporarily unavailable: %s. "
                    "Retrying while the driver container keeps running.",
                    getattr(exc, "reason", str(exc)),
                )
                raise HTTPError(str(exc)) from exc
            raise

    def fetch_requested_container_logs(
        self,
        pod,
        containers: Iterable[str] | str | Literal[True],
        follow_logs=False,
        container_name_log_prefix_enabled: bool = True,
        log_formatter=None,
    ) -> list[PodLoggingStatus]:
        try:
            return super().fetch_requested_container_logs(
                pod=pod,
                containers=containers,
                follow_logs=follow_logs,
                container_name_log_prefix_enabled=container_name_log_prefix_enabled,
                log_formatter=log_formatter,
            )
        except ApiException as exc:
            if not _is_transient_log_exception(exc):
                raise

            container_name = self._primary_container_name(pod, containers)
            if not container_name:
                raise

            if self.container_is_running(
                pod, container_name=container_name
            ) or self.container_is_terminated(pod, container_name=container_name):
                self.log.warning(
                    "Spark driver log stream failed with a transient Kubernetes API "
                    "error. Keeping the Airflow task attached to driver container "
                    "state; live logs may pause until the container exits. reason=%s",
                    getattr(exc, "reason", str(exc)),
                )
                self.await_container_completion(
                    pod=pod,
                    container_name=container_name,
                    polling_time=30,
                )
                return [PodLoggingStatus(running=False, last_log_time=None)]

            raise

    def _primary_container_name(
        self,
        pod,
        containers: Iterable[str] | str | Literal[True],
    ) -> str | None:
        if isinstance(containers, str):
            return containers
        if containers is True:
            names = self.get_container_names(pod)
            return names[0] if names else None
        for container in containers:
            return container
        return None


class SafeSparkKubernetesOperator(SparkKubernetesOperator):
    """SparkKubernetesOperator with retry-tolerant driver log streaming."""

    @cached_property
    def pod_manager(self) -> SafeSparkPodManager:
        return SafeSparkPodManager(kube_client=self.client)
