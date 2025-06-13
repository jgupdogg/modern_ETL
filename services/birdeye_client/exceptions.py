"""
BirdEye API Custom Exceptions

Provides specific exception types for different API error conditions.
"""


class BirdEyeAPIError(Exception):
    """Base exception for BirdEye API errors."""
    
    def __init__(self, message: str, status_code: int = None, response_data: dict = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class BirdEyeAuthError(BirdEyeAPIError):
    """Raised when API authentication fails."""
    pass


class BirdEyeRateLimitError(BirdEyeAPIError):
    """Raised when API rate limit is exceeded."""
    
    def __init__(self, message: str, retry_after: int = None, **kwargs):
        self.retry_after = retry_after
        super().__init__(message, **kwargs)


class BirdEyeValidationError(BirdEyeAPIError):
    """Raised when API request validation fails."""
    pass


class BirdEyeTimeoutError(BirdEyeAPIError):
    """Raised when API request times out."""
    pass