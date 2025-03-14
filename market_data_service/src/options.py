from typing import Optional, Dict, Any, List
import pandas as pd
from yahooquery import Ticker
import json
from mcp import McpError
from pydantic import Field

async def get_option_chain(
    symbol: str = Field(..., description="The stock symbol to get options data for"),
    expiration_date: str = Field(..., description="Expiration date in format 'YYYY-MM-DD'"),
    max_dollar_distance: int = Field(..., description="Max dollar distance from the current price. Options with a strike price within this distance will be returned."),
    option_type: Optional[str] = Field(None, description="Optional filter for option type ('calls' or 'puts')"),
    in_the_money: Optional[bool] = Field(None, description="Optional filter for in-the-money options"),
    min_absolute_delta: Optional[float] = Field(None, description="Optional filter for minimum absolute delta. Options with absolute delta greater than this value will be returned."),
    max_absolute_delta: Optional[float] = Field(None, description="Optional filter for maximum absolute delta. Options with absolute delta less than this value will be returned."),
) -> str:
    """
    Get option chain data for a given symbol using yahooquery.
    
    Args:
        symbol: The stock symbol to get options data for
        expiration_date: expiration date in format 'YYYY-MM-DD'
        max_dollar_distance: max dollar distance from the current price. Options with a strike price within this distance will be returned.
        option_type: Optional filter for option type ('calls' or 'puts')
        in_the_money: Optional filter for in-the-money options
        min_absolute_delta: Optional filter for minimum absolute delta. Options with absolute delta greater than this value will be returned.
        max_absolute_delta: Optional filter for maximum absolute delta. Options with absolute delta less than this value will be returned.
        
    Returns:
        json records with lines=true of a dataframe with the option chain data
    """
    # Initialize Ticker with the symbol
    ticker = Ticker(symbol)
    
    # Get the option chain data
    try:
        option_chain_df = ticker.option_chain
        
        # Check if we got valid data
        if option_chain_df is None or option_chain_df.empty:
            return json.dumps([])
        
        if expiration_date in option_chain_df.index.get_level_values('expiration'):
            option_chain_df = option_chain_df.xs(expiration_date, level='expiration')
        else:
            # Get available expiration dates
            available_dates = option_chain_df.index.get_level_values('expiration').unique().tolist()
            available_dates = [str(date.date()) for date in available_dates]
            raise McpError(f"Expiration date {expiration_date} not found. Available dates: {available_dates}")
        
        # Filter by option type if provided
        if option_type:
            if option_type.lower() in ['calls', 'puts']:
                option_chain_df = option_chain_df.xs(option_type.lower(), level='optionType')
            else:
                return json.dumps({"error": "Option type must be 'calls' or 'puts'"})
        
        # Filter by in-the-money status if provided
        if in_the_money is not None:
            option_chain_df = option_chain_df[option_chain_df['inTheMoney'] == in_the_money]
        
        # Filter by max dollar distance if provided
        current_price = float(ticker.price['regularMarketPrice'])
        option_chain_df = option_chain_df[option_chain_df['strike'] <= current_price + max_dollar_distance]
        option_chain_df = option_chain_df[option_chain_df['strike'] >= current_price - max_dollar_distance]
        
        # Convert DataFrame to JSON
        # Reset index to include the multi-index columns in the output
        option_chain_df = option_chain_df.reset_index()
        
        # Convert any datetime columns to string for JSON serialization
        for col in option_chain_df.columns:
            if pd.api.types.is_datetime64_any_dtype(option_chain_df[col]):
                option_chain_df[col] = option_chain_df[col].dt.strftime('%Y-%m-%d')
        
        # Return as JSON
        return option_chain_df.to_json(orient="records")
        
    except Exception as e:
        return json.dumps({"error": f"Error fetching option chain: {str(e)}"})


async def get_option_expirations(
    symbol: str = Field(..., description="The stock symbol to get option expiration dates for")
) -> str:
    """
    Get available option expiration dates for a given symbol.
    
    Args:
        symbol: The stock symbol to get option expiration dates for
        
    Returns:
        JSON string containing the available expiration dates,
        example:
        ["2023-01-01", "2023-01-02", "2023-01-03"]
        If no option data is available, return an empty list.
    """
    ticker = Ticker(symbol)
    option_chain_df = ticker.option_chain
    
    if option_chain_df is None or option_chain_df.empty:
        return json.dumps([])
    
    # Extract unique expiration dates
    expiration_dates = option_chain_df.index.get_level_values('expiration').unique()
    expiration_dates = [str(date.date()) for date in expiration_dates]
    
    return json.dumps(expiration_dates)
