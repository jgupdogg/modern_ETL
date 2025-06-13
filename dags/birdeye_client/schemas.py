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
    field_mapping = {
        'address': 'token_address',
        'logoURI': 'logo_uri',
        'mc': 'market_cap',
        'v1hUSD': 'volume_1h_usd',
        'v2hUSD': 'volume_2h_usd',
        'v4hUSD': 'volume_4h_usd',
        'v8hUSD': 'volume_8h_usd',
        'v24hUSD': 'volume_24h_usd',
        'v1hChangePercent': 'volume_1h_change_percent',
        'v2hChangePercent': 'volume_2h_change_percent',
        'v4hChangePercent': 'volume_4h_change_percent',
        'v8hChangePercent': 'volume_8h_change_percent',
        'v24hChangePercent': 'volume_24h_change_percent',
        'priceChange1hPercent': 'price_change_1h_percent',
        'priceChange2hPercent': 'price_change_2h_percent',
        'priceChange4hPercent': 'price_change_4h_percent',
        'priceChange8hPercent': 'price_change_8h_percent',
        'priceChange24hPercent': 'price_change_24h_percent',
        'trade1h': 'trade_1h_count',
        'trade2h': 'trade_2h_count',
        'trade4h': 'trade_4h_count',
        'trade8h': 'trade_8h_count',
        'trade24h': 'trade_24h_count',
        'lastTradeUnixTime': 'last_trade_unix_time',
        'recentListingTime': 'recent_listing_time',
    }
    
    # Apply field mapping
    for api_field, schema_field in field_mapping.items():
        if api_field in raw_data:
            normalized[schema_field] = raw_data[api_field]
    
    # Direct field copies (same name in API and schema)
    direct_fields = [
        'symbol', 'name', 'decimals', 'liquidity', 'price', 'fdv',
        'holder', 'extensions'
    ]
    
    for field in direct_fields:
        if field in raw_data:
            normalized[field] = raw_data[field]
    
    return normalized