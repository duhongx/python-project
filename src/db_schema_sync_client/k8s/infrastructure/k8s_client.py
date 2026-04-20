"""Kubernetes API client wrapper.

Wraps the official `kubernetes` Python SDK to provide the operations needed by
this application: listing deployments, reading container images, and patching
image versions for rollback.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, List, Optional

from db_schema_sync_client.k8s.domain.models import ContainerImage, DeploymentInfo


class K8sClient:
    """Thin wrapper around the kubernetes SDK AppsV1Api."""

    def __init__(self, kubeconfig_path: Path, context_name: str) -> None:
        self._kubeconfig_path = str(kubeconfig_path)
        self._context_name = context_name
        self._apps_v1 = None

    def _get_api(self):
        if self._apps_v1 is None:
            from kubernetes import client, config as k8s_config  # type: ignore[import]

            # Load kubeconfig into a dedicated Configuration object so we can
            # disable SSL verification for clusters that use self-signed certs.
            configuration = client.Configuration()
            k8s_config.load_kube_config(
                config_file=self._kubeconfig_path,
                context=self._context_name,
                client_configuration=configuration,
            )
            configuration.verify_ssl = False
            # Suppress the urllib3 InsecureRequestWarning in the console
            import urllib3  # type: ignore[import]
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            api_client = client.ApiClient(configuration=configuration)
            self._apps_v1 = client.AppsV1Api(api_client=api_client)
        return self._apps_v1

    @staticmethod
    def list_contexts(kubeconfig_path: Path) -> List[str]:
        """Return all context names found in a kubeconfig file."""
        from kubernetes import config as k8s_config  # type: ignore[import]

        contexts, _ = k8s_config.list_kube_config_contexts(config_file=str(kubeconfig_path))
        return [ctx["name"] for ctx in contexts]

    def list_deployments(self, namespace: str) -> List[DeploymentInfo]:
        """Fetch all Deployments in *namespace* and return their live image info."""
        api = self._get_api()
        result = api.list_namespaced_deployment(namespace=namespace)
        deployments: List[DeploymentInfo] = []
        for item in result.items:
            containers = [
                ContainerImage(container_name=c.name, image=c.image)
                for c in (item.spec.template.spec.containers or [])
            ]
            status = item.status or {}
            deployments.append(
                DeploymentInfo(
                    name=item.metadata.name,
                    namespace=namespace,
                    replicas=item.spec.replicas or 0,
                    ready_replicas=getattr(item.status, "ready_replicas", 0) or 0,
                    containers=containers,
                )
            )
        return deployments

    def patch_deployment_images(
        self,
        namespace: str,
        deployment_name: str,
        container_images: List[ContainerImage],
    ) -> None:
        """Patch the given deployment so each container uses the specified image.

        Uses strategic merge patch so only the listed containers are modified.
        """
        api = self._get_api()
        patch_body = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {"name": ci.container_name, "image": ci.image}
                            for ci in container_images
                        ]
                    }
                }
            }
        }
        api.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=patch_body,
        )

    def wait_for_rollout(
        self,
        namespace: str,
        deployment_name: str,
        timeout: int = 300,
        poll_interval: int = 3,
        progress_cb: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """Poll until the deployment rollout completes (all pods Running/Ready).

        Returns True when fully ready, False on timeout.
        A deployment is considered ready when:
          - updated_replicas >= spec.replicas
          - ready_replicas   >= spec.replicas
          - available_replicas >= spec.replicas
          - unavailable_replicas == 0
        """
        api = self._get_api()
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            dep = api.read_namespaced_deployment(
                name=deployment_name, namespace=namespace
            )
            spec_replicas = dep.spec.replicas or 1
            updated = getattr(dep.status, "updated_replicas", 0) or 0
            ready = getattr(dep.status, "ready_replicas", 0) or 0
            available = getattr(dep.status, "available_replicas", 0) or 0
            unavailable = getattr(dep.status, "unavailable_replicas", 0) or 0

            if progress_cb:
                progress_cb(
                    f"{deployment_name}: {ready}/{spec_replicas} Running"
                )

            if (
                updated >= spec_replicas
                and ready >= spec_replicas
                and available >= spec_replicas
                and unavailable == 0
            ):
                return True

            time.sleep(poll_interval)

        # Final status on timeout
        if progress_cb:
            dep = api.read_namespaced_deployment(
                name=deployment_name, namespace=namespace
            )
            spec_replicas = dep.spec.replicas or 1
            ready = getattr(dep.status, "ready_replicas", 0) or 0
            progress_cb(
                f"{deployment_name}: 等待超时，当前 {ready}/{spec_replicas} Running"
            )
        return False
