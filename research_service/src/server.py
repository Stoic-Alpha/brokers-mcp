from datetime import datetime
from logging import getLogger
from urllib.error import HTTPError
from common_lib.mcp import get_current_market_time, is_realtime
from mcp.server.fastmcp import FastMCP
from pydantic import Field
import tradingview_screener
from tradingview_screener import *
from async_screener import Query, Scanner
from column_values_index import index
from scanner import QUERY_LANGUAGE_DOCS
import traceback


logger = getLogger(__name__)

mcp = FastMCP("Research Service")

@mcp.tool(
    name="scan_from_scanner",
    description=f"""Use a scanner from the built-in scanner list.

Available lists: {tradingview_screener.Scanner.names()}"""
)
async def scan_from_scanner(
    list_name: str = Field(..., description="Name of the built-in scanner to use")
) -> str:
    """
    Use a scanner from the built-in scanner list to get stock screening results.

    Returns:
        str: Scanner results as a string
    """
    try:
        result = (await getattr(Scanner, list_name).async_get_scanner_data())[1]
        return str(result)
    except Exception as e:
        logger.error("Error while executing query: %s\nStack trace: %s", repr(e), traceback.format_exc())
        raise ValueError("Error while executing query: " + repr(e))

@mcp.tool(name="search_available_columns")
async def search_available_columns(
    queries: list[str] = Field(..., description="Search terms to find matching column names")
) -> str:
    """
    Search for screener columns in TradingView Screener. Tradingview screener has over 3K 
    different columns that can be used to screen stocks. This tool can be used to find multiple
    columns at once by utilizing a list of query terms.
    Query terms should be short and consist of a single word or phrase.
    It is encouraged to use as many query terms as possible to find the desired columns.
    For example: 
    If you want to find columns that represent change and/or market cap, you can use the query terms:
    ["change", "market", "cap"]

    Returns:
        str: Set of matching column names
    """
    if not queries:
        raise ValueError("The queries parameter is required")

    matched_columns = []
    for query in queries:
        matched_columns.extend(index.search(query))
    if len(matched_columns) == 0:
        return "No columns found, try a different search query"
    else:
        return str(matched_columns)


@mcp.tool(
    name="scan_for_stocks",
)
async def scan_for_stocks(
    query: str = Field(..., description="An instance of `Query` from TradingView Screener query language in Python")
) -> str:
    """
    Scan for stocks using a declarative query language utilizing TradingView's Screener API.
    Returns:
        str: Scanner results dataframe as a string
    """
    try:
        query_object = eval(query)
        try:
            result = (await query_object.async_get_scanner_data())[1]
            return str(result)
        except HTTPError as err:
            if "unknown field" in err.message.lower():
                return (f"Unknown field in query: {query}, query the available columns"
                       f" with search_available_columns and try again with a valid column")
            else:
                logger.error("Error while executing query: %s\nStack trace: %s", repr(err), traceback.format_exc())
                raise ValueError(f"Error while executing query: {err}")
    except Exception as e:
        logger.error("Error while executing query: %s\nStack trace: %s", repr(e), traceback.format_exc())
        raise ValueError("Error while executing query: " + repr(e))
    

@mcp.tool(description="Get summaries of important metrics for given symbols")
async def get_symbol_summaries(
    symbols: str = Field(..., description="Comma-separated list of stock symbols to get summaries for")
) -> str:
    """
    Get summaries of important metrics for given symbols.

    Returns:
        str: CSV string containing summary data for the requested symbols
    """
    symbol_list = [s.strip() for s in symbols.split(",")]
    if is_realtime():
        columns = [
            "name", "description", "close", "volume", "market_cap_basic",
            "price_52_week_high", "price_52_week_low", "High.3M", "Low.3M",
            "postmarket_high", "postmarket_low", "premarket_high", "premarket_low",
            "VWAP", "industry", "sector", "change_from_open", "change", "Perf.1M", "Perf.3M",
            "float_shares_outstanding", "gap", "oper_income_fy", "earnings_release_next_date", "Recommend.All"
        ]
    else:
        columns = [
            "name", "description", "market_cap_basic","industry", "sector","float_shares_outstanding", "oper_income_fy"
        ]

    query = (Query()
        .select(*columns)
        .where(Column("name").isin(symbol_list))
    )
    result = (await query.async_get_scanner_data())[1]
    try:
        result["earnings_release_next_date"] = datetime.fromtimestamp(result["earnings_release_next_date"].iloc[0]).strftime("%Y-%m-%d")
    except:
        pass
    
    def format_technical_rating(rating: float) -> str:
        if rating >= 0.5:
            return 'Strong Buy'
        elif rating >= 0.1:
            return 'Buy'
        elif rating >= -0.1:
            return 'Neutral'
        elif rating >= -0.5:
            return 'Sell'
        else:
            return 'Strong Sell'

    if is_realtime():
        # todo: handle simulation data
        result["rating"] = result["Recommend.All"].apply(format_technical_rating)
        result.rename(columns={"change": "change_from_last_close_%", "change_from_open": "change_from_open_%", "close": "last"}, inplace=True)
        result.drop(columns=["Recommend.All"], inplace=True)

    result["market_cap_basic_millions"] = result["market_cap_basic"] // 1000000
    result.drop(columns=["oper_income_fy", "market_cap_basic"], inplace=True)
    for c in result.columns:
        if result[c].dtype == "float64":
            result[c] = result[c].round(2)

    return result.to_csv(index=False, na_rep="N/A")

@mcp.resource(uri="resource://get_symbol_summary/{symbol}", name="get_symbol_summary")
async def get_symbol_summary_resource(
    symbol: str = Field(..., description="Stock symbol to get summary for")
) -> str:
    """
    Get summary of important metrics for a single symbol.

    Returns:
        str: CSV string containing summary data for the requested symbol
    """
    return await get_symbol_summaries(symbol)

if __name__ == "__main__":
    mcp.run(transport="sse")
    