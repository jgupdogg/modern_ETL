"""
BirdEye API Client Package

Provides a standardized interface for interacting with BirdEye API endpoints.
Supports authentication, rate limiting, and comprehensive error handling.
"""

from .client import BirdEyeAPIClient
from .endpoints import BirdEyeEndpoints
from .schemas import TokenListSchema, TokenPriceSchema
from .exceptions import BirdEyeAPIError, BirdEyeRateLimitError, BirdEyeAuthError

__version__ = "1.0.0"

__all__ = [
    "BirdEyeAPIClient",
    "BirdEyeEndpoints", 
    "TokenListSchema",
    "TokenPriceSchema",
    "BirdEyeAPIError",
    "BirdEyeRateLimitError", 
    "BirdEyeAuthError"
]