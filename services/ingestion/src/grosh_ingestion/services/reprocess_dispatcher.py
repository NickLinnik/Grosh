"""Dispatcher that submits reprocess K8s Jobs on behalf of the ingestion service.

Both per-user (one user_id) and admin bulk (many user_ids) flow through the
same submit() method. The caller_user_id parameter controls whether a
grosh.app/user-id label is applied:

- Per-user trigger: caller_user_id = the authenticated user's UUID.
  Label applied; status endpoint can scope-check the job.
- Admin bulk: caller_user_id = None.
  No user-id label; status endpoint verifies absence of the label.
"""

import json
import logging
import os
import time
from uuid import UUID

import kubernetes.client
import urllib3.exceptions
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.errors import K8sDispatchError

K8S_JOB_SUBMIT_TIMEOUT_SECONDS = 10

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

    def submit(
        self,
        user_ids: list[UUID],
        caller_user_id: UUID | None,
    ) -> str:
        """Create a K8s reprocess Job for one or more users.

        user_ids: the list of users to reprocess (passed as USER_IDS_JSON env var).
        caller_user_id: the authenticated user for per-user triggers, or None for
            admin bulk. Controls presence of the grosh.app/user-id label.

        Returns the constructed job name.
        Raises K8sDispatchError if the API is not configured or the call fails.
        """
        if self._batch_api is None:
            raise K8sDispatchError("Cannot create reprocess job — K8s not configured")

        if caller_user_id is not None:
            short = str(caller_user_id).replace("-", "")[:8]
        else:
            short = "admin"

        job_name = f"grosh-reprocess-{short}-{int(time.time())}"

        labels: dict[str, str] = {
            "app.kubernetes.io/managed-by": "grosh-ingestion",
            "grosh.app/job-kind": "reprocess",
        }
        if caller_user_id is not None:
            labels["grosh.app/user-id"] = str(caller_user_id)

        job = kubernetes.client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=kubernetes.client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels=labels,
            ),
            spec=kubernetes.client.V1JobSpec(
                backoff_limit=0,
                active_deadline_seconds=7200,
                ttl_seconds_after_finished=3600,
                template=kubernetes.client.V1PodTemplateSpec(
                    spec=kubernetes.client.V1PodSpec(
                        service_account_name="grosh-ingestion",
                        restart_policy="Never",
                        containers=[
                            kubernetes.client.V1Container(
                                name="reprocess",
                                image=self._image,
                                image_pull_policy=self._image_pull_policy,
                                command=[
                                    "python",
                                    "-m",
                                    "grosh_normalization.reprocess_main",
                                ],
                                env=[
                                    kubernetes.client.V1EnvVar(
                                        name="USER_IDS_JSON",
                                        value=json.dumps([str(u) for u in user_ids]),
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
                _request_timeout=K8S_JOB_SUBMIT_TIMEOUT_SECONDS,
            )
        except (ApiException, urllib3.exceptions.TimeoutError) as exc:
            raise K8sDispatchError(f"Cannot create reprocess job: {exc}") from exc

        logger.info(
            "Created K8s reprocess Job %s (user_ids=%s, caller=%s)",
            job_name,
            [str(u) for u in user_ids],
            caller_user_id,
        )
        return job_name
