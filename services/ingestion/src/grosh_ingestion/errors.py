"""Domain exceptions for the ingestion service."""


class AccountAlreadyExistsError(Exception):
    pass


class AccountNotOwnedError(Exception):
    pass


class AccountNotManualError(Exception):
    """Raised when a /v1/manual/* endpoint is invoked against a non-manual account.

    Manual endpoints (create_transaction, update_account, delete_account) only
    operate on accounts whose source is 'manual'. Bank-connected accounts must
    be modified through their bank's integration.
    """


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
