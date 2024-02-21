class FuncxError(Exception):
    """Base class for all funcx exceptions"""

    def __str__(self):
        return self.__repr__()


class RegistrationError(FuncxError):
    """Registering the endpoint has failed"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Endpoint registration failed due to {self.reason}"


class FuncXUnreachable(FuncxError):
    """FuncX remote service is unreachable"""

    def __init__(self, address):
        self.address = address

    def __repr__(self):
        return f"FuncX remote service is un-reachable at {self.address}"


class MalformedResponse(FuncxError):
    """FuncX remote service responded with a Malformed Response"""

    def __init__(self, response):
        self.response = response

    def __repr__(self):
        return "FuncX remote service responded with Malformed Response: {}".format(
            self.response
        )


class FailureResponse(FuncxError):
    """FuncX remote service responded with a failure"""

    def __init__(self, response):
        self.response = response

    def __repr__(self):
        return "FuncX remote service failed to fulfill request: {}".format(
            self.response
        )


class VersionMismatch(FuncxError):
    """Either client and endpoint version mismatch, or version cannot be retrieved."""

    def __init__(self, version_message):
        self.version_message = version_message

    def __repr__(self):
        return f"FuncX Versioning Issue: {self.version_message}"


class SerializationError(FuncxError):
    """Something failed during serialization or deserialization."""

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f"Serialization Error during: {self.message}"


class UserCancelledException(FuncxError):
    """User cancelled execution"""

    def __repr__(self):
        return "Task Exception: User Cancelled Execution"


class InvalidScopeException(FuncxError):
    """Invalid API Scope"""

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f"Invalid Scope: {self.message}"


class HTTPError(FuncxError):
    """An HTTP Request Failed"""

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f"HTTP request failed: {self.message}"


class TaskPending(FuncxError):
    """Task is pending and no result is available yet"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Task is pending due to {self.reason}"
