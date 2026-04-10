class DomainError(Exception):
    """Base class for all service-layer domain errors.

    Services raise these classes to signal business rule violations. The
    HTTP translation layer (``error_handlers.py``) catches each subclass
    and maps it to a response. Non-HTTP callers (workers, CLI tools) can
    catch ``DomainError`` directly.
    """
