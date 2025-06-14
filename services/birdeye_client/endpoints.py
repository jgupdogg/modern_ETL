"""
BirdEye API Endpoints

Centralized endpoint definitions and URL builders for BirdEye API.
"""

from typing import Dict, Any, Optional
from urllib.parse import urlencode


class BirdEyeEndpoints:
    """BirdEye API endpoint definitions and URL builders."""
    
    BASE_URL = "https://public-api.birdeye.so"
    
    # Token endpoints
    TOKEN_LIST_V3 = "/defi/v3/token/list"
    TOKEN_LIST_V2 = "/defi/tokenlist"
    TOKEN_PRICE = "/defi/v2/tokens/price"
    TOKEN_PRICE_MULTIPLE = "/defi/v2/tokens/price_multiple"
    TOKEN_HISTORY = "/defi/v2/tokens/history"
    TOKEN_METADATA = "/defi/v2/tokens/metadata"
    TOKEN_TRENDING = "/defi/v2/tokens/trending"
    TOKEN_CREATION_INFO = "/defi/v2/tokens/creation_info"
    
    # V3 Token endpoints
    TOKEN_METADATA_V3 = "/defi/v3/token/meta-data/single"
    TOKEN_MARKET_DATA_V3 = "/defi/v3/token/market-data"
    TOKEN_TRADE_DATA_V3 = "/defi/v3/token/trade-data/single"
    TOKEN_HOLDERS_V3 = "/defi/v3/token/holder"
    
    # Security endpoints
    TOKEN_SECURITY = "/defi/v2/tokens/security"
    
    # Market data endpoints
    TOKEN_VOLUME = "/defi/v2/tokens/volume"
    TOKEN_LIQUIDITY = "/defi/v2/tokens/liquidity"
    TOKEN_HOLDERS = "/defi/v2/tokens/holders"
    
    # DEX endpoints
    TRADES_TOKEN = "/defi/v2/trades/token"
    TRADES_PAIR = "/defi/v2/trades/pair"
    
    # Wallet endpoints
    WALLET_PORTFOLIO = "/v1/wallet/portfolio"
    WALLET_TRANSACTIONS = "/defi/v3/wallet/trade-history"
    
    @classmethod
    def build_url(cls, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Build complete URL with optional query parameters."""
        url = f"{cls.BASE_URL}{endpoint}"
        if params:
            # Filter out None values
            clean_params = {k: v for k, v in params.items() if v is not None}
            if clean_params:
                url += f"?{urlencode(clean_params)}"
        return url
    
    @classmethod
    def get_token_list_url(cls, **params) -> str:
        """Build URL for token list endpoint."""
        return cls.build_url(cls.TOKEN_LIST_V3, params)
    
    @classmethod
    def get_token_price_url(cls, address: str, **params) -> str:
        """Build URL for single token price endpoint."""
        params['address'] = address
        return cls.build_url(cls.TOKEN_PRICE, params)
    
    @classmethod
    def get_token_metadata_url(cls, address: str, **params) -> str:
        """Build URL for token metadata endpoint."""
        params['address'] = address
        return cls.build_url(cls.TOKEN_METADATA, params)
    
    @classmethod
    def get_token_history_url(cls, address: str, **params) -> str:
        """Build URL for token price history endpoint."""
        params['address'] = address
        return cls.build_url(cls.TOKEN_HISTORY, params)