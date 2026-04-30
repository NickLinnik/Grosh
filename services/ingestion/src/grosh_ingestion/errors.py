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
