"""Domain exceptions for the ingestion service."""


class AccountAlreadyExistsError(Exception):
    pass


class AccountNotOwnedError(Exception):
    pass


class InvalidRateSourceError(Exception):
    pass


class IntegrationAlreadyExistsError(Exception):
    pass


class BackfillAlreadyRunningError(Exception):
    pass


class K8sDispatchError(RuntimeError):
    """Raised when the ingestion service cannot dispatch a K8s Job.

    Reasons include: K8s API unreachable, config missing, RBAC denied, etc.
    """
