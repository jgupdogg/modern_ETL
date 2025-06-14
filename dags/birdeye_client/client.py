"""
BirdEye API Client

Provides a standardized interface for interacting with BirdEye API endpoints.
Supports authentication, rate limiting, error handling, and response validation.
"""

import time
import logging
from typing import Dict, Any, List, Optional, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .endpoints import BirdEyeEndpoints
from .exceptions import (
    BirdEyeAPIError, 
    BirdEyeAuthError, 
    BirdEyeRateLimitError,
    BirdEyeValidationError,
    BirdEyeTimeoutError
)
from .schemas import normalize_token_data


class BirdEyeAPIClient:
    """
    BirdEye API Client with comprehensive error handling and rate limiting.
    
    Features:
    - Automatic retries with exponential backoff
    - Rate limiting to respect API limits
    - Request/response logging
    - Custom exception handling
    - Response validation and normalization
    """
    
    def __init__(
        self, 
        api_key: str,
        rate_limit_delay: float = 0.5,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.3
    ):
        """
        Initialize BirdEye API client.
        
        Args:
            api_key: BirdEye API key
            rate_limit_delay: Delay between requests in seconds
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Backoff factor for retries
        """
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=backoff_factor,
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json',
            'User-Agent': 'Claude-Pipeline/1.0'
        })
        
        self._last_request_time = 0
    
    def _respect_rate_limit(self) -> None:
        """Ensure rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def _make_request(
        self, 
        method: str, 
        url: str, 
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make HTTP request with error handling and rate limiting.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Query parameters
            **kwargs: Additional requests arguments
            
        Returns:
            Response object
            
        Raises:
            BirdEyeAPIError: For various API errors
        """
        self._respect_rate_limit()
        
        try:
            self.logger.debug(f"Making {method} request to {url}")
            if params:
                self.logger.debug(f"Request params: {params}")
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                timeout=self.timeout,
                **kwargs
            )
            
            self.logger.debug(f"Response status: {response.status_code}")
            
            # Handle specific error codes
            if response.status_code == 401:
                raise BirdEyeAuthError(
                    "Authentication failed. Check your API key.",
                    status_code=response.status_code
                )
            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                raise BirdEyeRateLimitError(
                    "Rate limit exceeded",
                    status_code=response.status_code,
                    retry_after=int(retry_after) if retry_after else None
                )
            elif response.status_code == 400:
                raise BirdEyeValidationError(
                    f"Request validation failed: {response.text}",
                    status_code=response.status_code,
                    response_data=response.json() if response.headers.get('content-type', '').startswith('application/json') else None
                )
            elif not response.ok:
                raise BirdEyeAPIError(
                    f"API request failed with status {response.status_code}: {response.text}",
                    status_code=response.status_code
                )
            
            return response
            
        except requests.exceptions.Timeout:
            raise BirdEyeTimeoutError(f"Request timed out after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise BirdEyeAPIError(f"Request failed: {str(e)}")
    
    def get_token_list(
        self,
        sort_by: str = "liquidity",
        sort_type: str = "desc",
        offset: int = 0,
        limit: int = 100,
        **filter_params
    ) -> Dict[str, Any]:
        """
        Get filtered token list from BirdEye API.
        
        Args:
            sort_by: Sort field (liquidity, volume, price, etc.)
            sort_type: Sort direction (asc, desc)
            offset: Pagination offset
            limit: Number of tokens to return (max 100)
            **filter_params: Additional filter parameters
            
        Returns:
            API response with token list data
        """
        params = {
            'sort_by': sort_by,
            'sort_type': sort_type,
            'offset': offset,
            'limit': min(limit, 100),  # API max is 100
            **filter_params
        }
        
        url = BirdEyeEndpoints.get_token_list_url()
        response = self._make_request('GET', url, params=params)
        
        return response.json()
    
    def get_token_price(self, token_address: str, **params) -> Dict[str, Any]:
        """
        Get current price for a specific token.
        
        Args:
            token_address: Token contract address
            **params: Additional parameters
            
        Returns:
            Token price data
        """
        url = BirdEyeEndpoints.get_token_price_url(token_address, **params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_metadata(self, token_address: str, **params) -> Dict[str, Any]:
        """
        Get metadata for a specific token.
        
        Args:
            token_address: Token contract address
            **params: Additional parameters
            
        Returns:
            Token metadata
        """
        url = BirdEyeEndpoints.get_token_metadata_url(token_address, **params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_history(
        self, 
        token_address: str, 
        time_from: Optional[int] = None,
        time_to: Optional[int] = None,
        **params
    ) -> Dict[str, Any]:
        """
        Get price history for a specific token.
        
        Args:
            token_address: Token contract address
            time_from: Start timestamp (Unix)
            time_to: End timestamp (Unix)
            **params: Additional parameters
            
        Returns:
            Token price history data
        """
        if time_from:
            params['time_from'] = time_from
        if time_to:
            params['time_to'] = time_to
            
        url = BirdEyeEndpoints.get_token_history_url(token_address, **params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_multiple_token_prices(self, token_addresses: List[str]) -> Dict[str, Any]:
        """
        Get prices for multiple tokens in a single request.
        
        Args:
            token_addresses: List of token contract addresses
            
        Returns:
            Multiple token price data
        """
        params = {'list_address': ','.join(token_addresses)}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_PRICE_MULTIPLE, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_security(self, token_address: str) -> Dict[str, Any]:
        """
        Get token security information.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Token security data
        """
        params = {'address': token_address}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_SECURITY, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_creation_info(self, token_address: str) -> Dict[str, Any]:
        """
        Get token creation information.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Token creation data
        """
        params = {'address': token_address}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_CREATION_INFO, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_metadata_v3(self, token_address: str) -> Dict[str, Any]:
        """
        Get token metadata using V3 endpoint.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Token metadata from V3 API
        """
        params = {'address': token_address}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_METADATA_V3, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_market_data(self, token_address: str) -> Dict[str, Any]:
        """
        Get token market data.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Token market data
        """
        params = {'address': token_address}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_MARKET_DATA_V3, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_trade_data(self, token_address: str) -> Dict[str, Any]:
        """
        Get token trade data.
        
        Args:
            token_address: Token contract address
            
        Returns:
            Token trade data
        """
        params = {'address': token_address}
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_TRADE_DATA_V3, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_token_top_holders(self, token_address: str, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch top holders for a given token address.
        
        Args:
            token_address: Token contract address
            offset: Pagination offset
            limit: Number of holders to return (max 100)
            
        Returns:
            Top holders data
        """
        params = {
            'address': token_address,
            'offset': offset,
            'limit': min(limit, 100)  # API max is typically 100
        }
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.TOKEN_HOLDERS_V3, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def get_wallet_transactions(self, wallet_address: str, limit: int = 100, **params) -> Dict[str, Any]:
        """
        Get trade history for a specific wallet address.
        
        Args:
            wallet_address: Wallet address to get trades for
            limit: Number of trades to return (max 100)
            **params: Additional parameters
            
        Returns:
            Wallet trade history data
        """
        params['wallet'] = wallet_address
        params['limit'] = min(limit, 100)  # API max is typically 100
        
        url = BirdEyeEndpoints.build_url(BirdEyeEndpoints.WALLET_TRANSACTIONS, params)
        response = self._make_request('GET', url)
        
        return response.json()
    
    def normalize_token_list_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize token list API response to consistent format.
        
        Args:
            response: Raw API response
            
        Returns:
            List of normalized token data
        """
        if 'data' not in response:
            return []
        
        # BirdEye API returns tokens under 'items' key, not 'tokens'
        tokens_data = response['data'].get('items', response['data'].get('tokens', []))
        
        if not tokens_data:
            return []
        
        normalized_tokens = []
        for token in tokens_data:
            normalized = normalize_token_data(token)
            normalized_tokens.append(normalized)
        
        return normalized_tokens