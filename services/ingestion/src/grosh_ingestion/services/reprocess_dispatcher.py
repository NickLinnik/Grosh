import json
import logging
import os
import time
from uuid import UUID

import kubernetes.client
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.errors import K8sDispatchError

logger = logging.getLogger(__name__)


class ReprocessDispatcher:
    def __init__(self, batch_api: kubernetes.client.BatchV1Api | None) -> None:
        image = os.environ.get("REPROCESS_IMAGE")
        if not image:
            raise K8sDispatchError("REPROCESS_IMAGE env var is required but not set")
        self._image: str = image
        self._image_pull_policy: str = os.environ.get(
            "IMAGE_PULL_POLICY", "IfNotPresent"
        )
        self._namespace: str = os.environ.get("K8S_NAMESPACE", "grosh")
        self._batch_api = batch_api

    def trigger_reprocess(self, user_id: UUID) -> str:
        """Create a K8s Job to reprocess transactions for one user.

        Returns the Job name.
        Raises RuntimeError if K8s is not configured or the API call fails.
        """
        if self._batch_api is None:
            raise K8sDispatchError("Cannot create reprocess job — K8s not configured")

        job_name = f"grosh-reprocess-{user_id.hex[:8]}-{int(time.time())}"
        job = kubernetes.client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=kubernetes.client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels={
                    "app": "grosh-reprocess",
                    "user_id": str(user_id),
                },
            ),
            spec=kubernetes.client.V1JobSpec(
                backoff_limit=0,
                active_deadline_seconds=7200,
                ttl_seconds_after_finished=86400,
                template=kubernetes.client.V1PodTemplateSpec(
                    spec=kubernetes.client.V1PodSpec(
                        restart_policy="Never",
                        containers=[
                            kubernetes.client.V1Container(
                                name="reprocess",
                                image=self._image,
                                image_pull_policy=self._image_pull_policy,
                                command=[
                                    "python",
                                    "-m",
                                    "grosh_consumer.jobs.run_reprocess",
                                ],
                                env=[
                                    kubernetes.client.V1EnvVar(
                                        name="USER_IDS_JSON",
                                        value=json.dumps([str(user_id)]),
                                    ),
                                ],
                                env_from=[
                                    kubernetes.client.V1EnvFromSource(
                                        secret_ref=kubernetes.client.V1SecretEnvSource(
                                            name="grosh-secrets-consumer"
                                        )
                                    )
                                ],
                                resources=kubernetes.client.V1ResourceRequirements(
                                    requests={"cpu": "100m", "memory": "256Mi"},
                                    limits={"cpu": "500m", "memory": "512Mi"},
                                ),
                            )
                        ],
                    )
                ),
            ),
        )

        try:
            self._batch_api.create_namespaced_job(
                namespace=self._namespace,
                body=job,
            )
        except ApiException as exc:
            raise K8sDispatchError(f"Cannot create reprocess job: {exc}") from exc

        logger.info("Created K8s Job %s for reprocess (user_id=%s)", job_name, user_id)
        return job_name
