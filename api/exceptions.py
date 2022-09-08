class HttpRestException(Exception):
    """Exception for REST response errors based on status code"""

    pass


class HttpPermisionDeniedException(Exception):
    """HTTP permision denied error"""

    pass


class HttpNotFoundException(Exception):
    """HTTP not found error"""

    pass
