"""
BirdEye API Response Schemas

PyArrow schemas for different BirdEye API response types.
"""

import pyarrow as pa
from typing import Dict, Any, List, Optional


class TokenListSchema:
    """Schema for BirdEye token list API response."""
    
    @staticmethod
    def get_pyarrow_schema() -> pa.Schema:
        """Get PyArrow schema for token list data."""
        return pa.schema([
            # Basic token information
            pa.field("token_address", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("decimals", pa.int32(), nullable=True),
            pa.field("logo_uri", pa.string(), nullable=True),
            
            # Market data
            pa.field("market_cap", pa.float64(), nullable=True),
            pa.field("fdv", pa.float64(), nullable=True),
            pa.field("liquidity", pa.float64(), nullable=True),
            pa.field("price", pa.float64(), nullable=True),
            
            # Volume metrics
            pa.field("volume_1h_usd", pa.float64(), nullable=True),
            pa.field("volume_2h_usd", pa.float64(), nullable=True),
            pa.field("volume_4h_usd", pa.float64(), nullable=True),
            pa.field("volume_8h_usd", pa.float64(), nullable=True),
            pa.field("volume_24h_usd", pa.float64(), nullable=True),
            
            # Volume change percentages
            pa.field("volume_1h_change_percent", pa.float64(), nullable=True),
            pa.field("volume_2h_change_percent", pa.float64(), nullable=True),
            pa.field("volume_4h_change_percent", pa.float64(), nullable=True),
            pa.field("volume_8h_change_percent", pa.float64(), nullable=True),
            pa.field("volume_24h_change_percent", pa.float64(), nullable=True),
            
            # Price change percentages
            pa.field("price_change_1h_percent", pa.float64(), nullable=True),
            pa.field("price_change_2h_percent", pa.float64(), nullable=True),
            pa.field("price_change_4h_percent", pa.float64(), nullable=True),
            pa.field("price_change_8h_percent", pa.float64(), nullable=True),
            pa.field("price_change_24h_percent", pa.float64(), nullable=True),
            
            # Trade counts
            pa.field("trade_1h_count", pa.int64(), nullable=True),
            pa.field("trade_2h_count", pa.int64(), nullable=True),
            pa.field("trade_4h_count", pa.int64(), nullable=True),
            pa.field("trade_8h_count", pa.int64(), nullable=True),
            pa.field("trade_24h_count", pa.int64(), nullable=True),
            
            # Additional metrics
            pa.field("holder", pa.int64(), nullable=True),
            pa.field("last_trade_unix_time", pa.int64(), nullable=True),
            pa.field("recent_listing_time", pa.int64(), nullable=True),
            pa.field("extensions", pa.string(), nullable=True),
            
            # Metadata
            pa.field("ingested_at", pa.timestamp('us', tz='UTC'), nullable=False),
            pa.field("batch_id", pa.string(), nullable=False),
        ])


class TokenPriceSchema:
    """Schema for BirdEye token price API response."""
    
    @staticmethod
    def get_pyarrow_schema() -> pa.Schema:
        """Get PyArrow schema for token price data."""
        return pa.schema([
            pa.field("token_address", pa.string(), nullable=False),
            pa.field("price", pa.float64(), nullable=True),
            pa.field("price_change_24h", pa.float64(), nullable=True),
            pa.field("price_change_24h_percent", pa.float64(), nullable=True),
            pa.field("volume_24h", pa.float64(), nullable=True),
            pa.field("market_cap", pa.float64(), nullable=True),
            pa.field("liquidity", pa.float64(), nullable=True),
            pa.field("last_updated", pa.timestamp('us', tz='UTC'), nullable=False),
        ])


class TokenMetadataSchema:
    """Schema for BirdEye token metadata API response."""
    
    @staticmethod
    def get_pyarrow_schema() -> pa.Schema:
        """Get PyArrow schema for token metadata."""
        return pa.schema([
            pa.field("token_address", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("decimals", pa.int32(), nullable=True),
            pa.field("logo_uri", pa.string(), nullable=True),
            pa.field("description", pa.string(), nullable=True),
            pa.field("website", pa.string(), nullable=True),
            pa.field("twitter", pa.string(), nullable=True),
            pa.field("telegram", pa.string(), nullable=True),
            pa.field("discord", pa.string(), nullable=True),
            pa.field("creator_address", pa.string(), nullable=True),
            pa.field("creation_time", pa.timestamp('us', tz='UTC'), nullable=True),
            pa.field("fetched_at", pa.timestamp('us', tz='UTC'), nullable=False),
        ])


def normalize_token_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize raw API response to match schema fields."""
    normalized = {}
    
    # Map API response fields to schema fields
    # Updated to match actual BirdEye API response format
    field_mapping = {
        'address': 'token_address',
        'logo_uri': 'logo_uri',  # API uses snake_case now
        'logoURI': 'logo_uri',   # Fallback for camelCase
        'market_cap': 'market_cap',  # Direct match
        'mc': 'market_cap',      # Fallback for old format
        # Handle actual API field names (snake_case)
        'last_trade_unix_time': 'last_trade_unix_time',
        'recent_listing_time': 'recent_listing_time',
        # Legacy field mappings (camelCase) for backward compatibility
        'lastTradeUnixTime': 'last_trade_unix_time',
        'recentListingTime': 'recent_listing_time',
    }
    
    # Apply field mapping
    for api_field, schema_field in field_mapping.items():
        if api_field in raw_data:
            normalized[schema_field] = raw_data[api_field]
    
    # Direct field copies (API already uses correct field names)
    direct_fields = [
        'symbol', 'name', 'decimals', 'liquidity', 'price', 'fdv',
        'holder', 'extensions',
        # Volume fields (API uses snake_case already)
        'volume_1h_usd', 'volume_2h_usd', 'volume_4h_usd', 'volume_8h_usd', 'volume_24h_usd',
        # Volume change fields
        'volume_1h_change_percent', 'volume_2h_change_percent', 'volume_4h_change_percent',
        'volume_8h_change_percent', 'volume_24h_change_percent',
        # Price change fields  
        'price_change_1h_percent', 'price_change_2h_percent', 'price_change_4h_percent',
        'price_change_8h_percent', 'price_change_24h_percent',
        # Trade count fields
        'trade_1h_count', 'trade_2h_count', 'trade_4h_count', 'trade_8h_count', 'trade_24h_count'
    ]
    
    for field in direct_fields:
        if field in raw_data:
            normalized[field] = raw_data[field]
    
    return normalized