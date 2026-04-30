import logging
import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.config import ConfigException

from grosh_ingestion.errors import BackfillAlreadyRunningError

logger = logging.getLogger(__name__)


class BackfillService:
    def __init__(self) -> None:
        try:
            k8s_config.load_incluster_config()
            self._batch_api: k8s_client.BatchV1Api | None = k8s_client.BatchV1Api()
        except ConfigException:
            try:
                k8s_config.load_kube_config()
                self._batch_api = k8s_client.BatchV1Api()
            except ConfigException:
                logger.warning(
                    "No K8s config found — BackfillService running"
                    " without cluster access"
                )
                self._batch_api = None
        self._image = os.environ.get("INGESTION_IMAGE", "grosh-ingestion:latest")
        self._image_pull_policy = os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent")
        self._namespace = os.environ.get("K8S_NAMESPACE", "grosh")

    def _has_active_job(self, label_selector: str) -> bool:
        """Return True if any non-completed, non-exhausted Job matches the selector."""
        if self._batch_api is None:
            return False
        jobs = self._batch_api.list_namespaced_job(
            namespace=self._namespace,
            label_selector=label_selector,
        )
        for job in jobs.items:
            if job.status.completion_time is None and (job.status.failed or 0) < (
                job.spec.backoff_limit or 1
            ):
                return True
        return False

    def trigger_transactions_backfill(
        self,
        integration_id: UUID,
        user_id: UUID,
        account_external_id: str,
        from_timestamp: int,
        to_timestamp: int,
    ) -> str:
        """Create a K8s Job to backfill historical transactions.

        Returns the Job name.
        Raises BackfillAlreadyRunningError if a job for this integration
        is already active.
        Raises RuntimeError if K8s is not configured.
        """
        if self._batch_api is None:
            raise RuntimeError("Cannot create backfill job — K8s not configured")

        label_selector = (
            f"app=grosh-transactions-backfill,integration-id={integration_id}"
        )
        if self._has_active_job(label_selector):
            raise BackfillAlreadyRunningError(
                f"A transaction backfill job for integration"
                f" {integration_id} is already running"
            )

        ts = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
        job_name = f"transactions-backfill-{ts}-{uuid4().hex[:8]}"
        job = k8s_client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels={
                    "app": "grosh-transactions-backfill",
                    "integration-id": str(integration_id),
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=3,
                ttl_seconds_after_finished=3600,
                active_deadline_seconds=7200,
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        service_account_name="grosh-ingestion",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="backfill",
                                image=self._image,
                                image_pull_policy=self._image_pull_policy,
                                command=[
                                    "python",
                                    "-m",
                                    "grosh_ingestion.jobs.run_transactions_backfill",
                                ],
                                env=[
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_INTEGRATION_ID",
                                        value=str(integration_id),
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_USER_ID",
                                        value=str(user_id),
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_ACCOUNT_EXTERNAL_ID",
                                        value=account_external_id,
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_FROM_TIMESTAMP",
                                        value=str(from_timestamp),
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_TO_TIMESTAMP",
                                        value=str(to_timestamp),
                                    ),
                                ],
                                env_from=[
                                    k8s_client.V1EnvFromSource(
                                        secret_ref=k8s_client.V1SecretEnvSource(
                                            name="grosh-secrets"
                                        )
                                    )
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "100m", "memory": "128Mi"},
                                    limits={"cpu": "500m", "memory": "256Mi"},
                                ),
                            )
                        ],
                    )
                ),
            ),
        )
        self._batch_api.create_namespaced_job(namespace=self._namespace, body=job)
        logger.info("Created K8s Job %s for transaction backfill", job_name)
        return job_name

    def trigger_rates_backfill(
        self,
        source: str,
        from_date: str,
        to_date: str,
    ) -> str:
        """Create a K8s Job to backfill historical currency rates.

        Returns the Job name.
        Raises BackfillAlreadyRunningError if a job for this source
        is already active.
        Raises RuntimeError if K8s is not configured.
        """
        if self._batch_api is None:
            raise RuntimeError("Cannot create backfill job — K8s not configured")

        label_selector = f"app=grosh-rates-backfill,source={source}"
        if self._has_active_job(label_selector):
            raise BackfillAlreadyRunningError(
                f"A rates backfill job for source '{source}' is already running"
            )

        ts = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
        job_name = f"rates-backfill-{ts}-{uuid4().hex[:8]}"
        job = k8s_client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels={
                    "app": "grosh-rates-backfill",
                    "source": source,
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=3,
                ttl_seconds_after_finished=3600,
                active_deadline_seconds=3600,
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        service_account_name="grosh-ingestion",
                        restart_policy="Never",
                        containers=[
                            k8s_client.V1Container(
                                name="backfill",
                                image=self._image,
                                image_pull_policy=self._image_pull_policy,
                                command=[
                                    "python",
                                    "-m",
                                    "grosh_ingestion.jobs.run_rates_backfill",
                                ],
                                env=[
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_SOURCE", value=source
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_FROM_DATE", value=from_date
                                    ),
                                    k8s_client.V1EnvVar(
                                        name="BACKFILL_TO_DATE", value=to_date
                                    ),
                                ],
                                env_from=[
                                    k8s_client.V1EnvFromSource(
                                        secret_ref=k8s_client.V1SecretEnvSource(
                                            name="grosh-secrets"
                                        )
                                    )
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "100m", "memory": "128Mi"},
                                    limits={"cpu": "500m", "memory": "256Mi"},
                                ),
                            )
                        ],
                    )
                ),
            ),
        )
        self._batch_api.create_namespaced_job(namespace=self._namespace, body=job)
        logger.info("Created K8s Job %s for rate backfill", job_name)
        return job_name
