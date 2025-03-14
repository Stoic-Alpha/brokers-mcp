from datetime import datetime, timedelta
from io import StringIO
import logging
import math
from typing import Optional
from common_lib.alpaca_helpers.async_impl.stock_client import (
    AsyncStockHistoricalDataClient,
)
from common_lib.alpaca_helpers.env import AlpacaSettings
from common_lib.mcp import get_current_market_time, is_realtime
import pandas as pd
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from pandas.tseries.offsets import BDay, BusinessHour
from pydantic import Field
from ta.indicators import add_indicators_to_bars_df, indicator_min_bars_back, plot_bars
from mcp.server.fastmcp import Image
import asyncio
import json

# Initialize Alpaca client
settings = AlpacaSettings()
stock_client = AsyncStockHistoricalDataClient(settings.api_key, settings.api_secret)

SUPPORTED_INDICATORS = {
    "sma_{period}": {
        "df_columns": ["sma_{period}"]
    },
    "ema_{period}": {
        "df_columns": ["ema_{period}"]
    },
    "rsi_{window_period}": {
        "df_columns": ["rsi_{window_period}"]
    },
    "macd_{fast_period}_{slow_period}_{signal_period}": {
        "df_columns": [
            "macd_{fast_period}_{slow_period}_{signal_period}",
            "macd_signal_{fast_period}_{slow_period}_{signal_period}",
            "macd_histogram_{fast_period}_{slow_period}_{signal_period}"
        ]
    },
    "bbands_{window_period}_{num_std}": {
        "df_columns": [
            "bb_upper_{window_period}_{num_std}",
            "bb_middle_{window_period}_{num_std}",
            "bb_lower_{window_period}_{num_std}"
        ]
    }
}

logger = logging.getLogger(__name__)


def get_timeframe(unit: str, bar_size: int) -> TimeFrame:
    """Convert unit and bar_size to Alpaca TimeFrame"""
    unit = unit.upper()
    if unit == "MINUTE":
        if bar_size <= 59:
            return TimeFrame(amount=bar_size, unit=TimeFrameUnit.Minute)
        else:
            return TimeFrame(amount=bar_size // 60, unit=TimeFrameUnit.Hour)
    elif unit == "HOUR":
        return TimeFrame(amount=bar_size, unit=TimeFrameUnit.Hour)
    elif unit == "DAILY":
        return TimeFrame(amount=bar_size, unit=TimeFrameUnit.Day)
    elif unit == "WEEKLY":
        return TimeFrame(amount=bar_size, unit=TimeFrameUnit.Week)
    elif unit == "MONTHLY":
        return TimeFrame(amount=bar_size, unit=TimeFrameUnit.Month)
    else:
        raise ValueError(f"Unsupported unit: {unit}")


def default_bars_back(unit: str, bar_size: int) -> int:
    unit = unit.upper()
    if unit == "MINUTE" and bar_size < 5:
        return 120 // bar_size  # 2 hours
    elif unit == "MINUTE" and bar_size < 15:
        return 60 * 7 // bar_size  # 1 day
    elif unit == "MINUTE" and bar_size < 30:
        return 60 * 13 // bar_size  # 2 days
    elif unit == "HOUR":
        return 5 * 24 // bar_size  # 1 week
    elif unit == "DAILY":
        return 30 // bar_size  # 30 days
    elif unit == "WEEKLY":
        return 26 // bar_size  # 26 weeks
    elif unit == "MONTHLY":
        return 12 // bar_size  # 12 months
    else:
        raise ValueError(f"Unknown unit: {unit}")


def bars_back_to_datetime(
    unit: str, bar_size: int, bars_back: int
) -> datetime:
    now = get_current_market_time()
    if unit == "Minute":
        total_minutes = bars_back * bar_size
        hours = (total_minutes // 60) + 1
        return bars_back_to_datetime("Hour", 1, hours)

    elif unit == "Hour":
        # 1 'business hour' bar => skip Sat/Sun
        interval = BusinessHour(n=bar_size, start="09:30", end="16:00")

    elif unit == "Daily":
        # 1 'business day' bar => skip Sat/Sun
        interval = BDay(n=bar_size)

    elif unit == "Weekly":
        # 1 'weekly' bar => treat that as 5 business days
        interval = BDay(n=5 * bar_size)

    elif unit == "Monthly":
        # Often approximate 21 business days per month
        interval = BDay(n=21 * bar_size)

    else:
        raise ValueError(f"Unknown unit: {unit}")
        
    return now - (interval * (bars_back + 10)) # the + 10 is a safety margin


async def get_alpaca_bars(
    symbol: str = Field(..., description="The symbol to fetch bars for"),
    unit: str = Field(..., description="Unit of time for the bars. Possible values are Minute, Hour, Daily, Weekly, Monthly."),
    bars_back: int = Field(..., description="Number of bars back to fetch."),
    bar_size: int = Field(..., description="Interval that each bar will consist of"),
    indicators: str = Field(default="", description=f"Optional indicators to plot, comma-separated. Supported: {list(SUPPORTED_INDICATORS.keys())}"),
    truncate_bars: bool = Field(default=True, description="Whether to truncate the bars to the requested bars back."),
    include_outside_hours: bool = Field(default=False, description="Whether to include data from pre-market and post-market sessions."),
) -> str:
    """Get historical bars data for a stock symbol in csv format"""
    timeframe = get_timeframe(unit, bar_size)
    original_bars_back = bars_back or default_bars_back(unit, bar_size)

    if indicators:
        min_bars_back = max(indicator_min_bars_back(i) for i in indicators.split(","))
        bars_back = min_bars_back + (bars_back or 0)
    else:
        bars_back = original_bars_back

    # Adjust bars_back to account for outside hours filtering
    adjusted_bars_back = bars_back
    if not include_outside_hours and unit.upper() in ["MINUTE", "HOUR"]:
        # For minute and hour bars, we need to adjust for outside market hours
        # Approximately 6.5 hours of market hours per day (9:30 AM - 4:00 PM)
        # vs 24 hours in a full day
        if unit.upper() == "MINUTE":
            # For minute bars, multiply by ~3.7 to account for filtering
            # (24/6.5 = ~3.7)
            adjusted_bars_back = int(bars_back * 3.7)
        elif unit.upper() == "HOUR":
            # For hour bars, multiply by ~4 to account for filtering
            # (24/6.5 = ~3.7, rounded up)
            adjusted_bars_back = int(bars_back * 4)
    
    start = bars_back_to_datetime(unit, bar_size, adjusted_bars_back)
    end = get_current_market_time()
    asof = None
    if not (is_realtime() and timeframe.unit in [TimeFrameUnit.Minute, TimeFrameUnit.Hour]):
        asof = get_current_market_time().strftime("%Y-%m-%d")
            
    # Create the request
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        adjustment="all",
        feed="sip",
        asof=asof,
    )

    # Get the bars
    bars_df = (await stock_client.get_stock_bars(request)).df
    if isinstance(bars_df.index, pd.MultiIndex):
        bars_df = bars_df.xs(symbol)

    if "trade_count" in bars_df.columns:
        bars_df.drop(columns=["trade_count"], inplace=True)
    if "volume" in bars_df.columns:
        bars_df["volume"] = bars_df["volume"].astype(int)
    # Convert index timezone to US/Eastern
    bars_df.index = bars_df.index.tz_convert("US/Eastern").tz_localize(None)

    # Filter out outside hours data if requested
    if not include_outside_hours and unit.upper() in ["MINUTE", "HOUR"]:
        # Keep only data from 9:30 AM to 4:00 PM ET on weekdays
        bars_df = bars_df[
            (bars_df.index.dayofweek < 5) &  # Monday to Friday
            (
                ((bars_df.index.hour == 9) & (bars_df.index.minute >= 30)) |  # 9:30 AM or later
                ((bars_df.index.hour > 9) & (bars_df.index.hour < 16)) |  # 10 AM to 3:59 PM
                ((bars_df.index.hour == 16) & (bars_df.index.minute == 0))  # 4:00 PM exactly
            )
        ]

    # Add indicators if requested
    if indicators:
        indicator_list = [i.strip() for i in indicators.split(",")]
        add_indicators_to_bars_df(bars_df, indicator_list)

    # Format datetime
    bars_df = bars_df.reset_index()
    bars_df["timestamp"] = bars_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    bars_df = bars_df.rename(columns={"timestamp": "datetime"})
    
    # If we still don't have enough bars after filtering, try again with a larger multiplier
    if not include_outside_hours and unit.upper() in ["MINUTE", "HOUR"] and len(bars_df) < original_bars_back:
        # If this is the first retry (we used the default multiplier)
        if adjusted_bars_back <= bars_back * 4:
            logger.info(f"Not enough bars after filtering ({len(bars_df)} < {original_bars_back}). Retrying with larger window.")
            return await get_alpaca_bars(
                symbol=symbol,
                unit=unit,
                bar_size=bar_size,
                indicators=indicators,
                bars_back=original_bars_back * 10,  # Use a much larger multiplier for the retry
                truncate_bars=truncate_bars,
                include_outside_hours=include_outside_hours,
            )
        else:
            # If we've already retried with a larger multiplier and still don't have enough,
            # just return what we have and log a warning
            logger.warning(
                f"Could not get {original_bars_back} bars for {symbol} with unit={unit}, "
                f"bar_size={bar_size}, include_outside_hours={include_outside_hours}. "
                f"Only got {len(bars_df)} bars."
            )
    
    if truncate_bars:
        bars_df = bars_df.iloc[-original_bars_back:] if len(bars_df) >= original_bars_back else bars_df
    
    for col in bars_df.columns:
        if bars_df[col].dtype == "float64":
            bars_df[col] = bars_df[col].round(3)

    return bars_df.to_csv(index=False, date_format="%Y-%m-%d %H:%M:%S")


async def plot_alpaca_bars_with_indicators(
    symbol: str = Field(..., description="The symbol to plot"),
    unit: str = Field(..., description="Unit of time for the bars. Possible values are Minute, Hour, Daily, Weekly, Monthly."),
    bars_back: int = Field(..., description="Number of bars back to fetch."),
    bar_size: int = Field(..., description="Interval that each bar will consist of"),
    indicators: Optional[str] = Field(default="", description=f"Optional indicators to plot, comma-separated. Supported: {list(SUPPORTED_INDICATORS.keys())}"),
    include_outside_hours: bool = Field(default=False, description="Whether to include data from pre-market and post-market sessions."),
) -> tuple[Image, str]:
    """Get a plot and a csv of the bars and indicators for a given symbol"""
    plot_bar_count = max(bars_back, default_bars_back(unit, bar_size))
    bars_df = pd.read_csv(
        StringIO(await get_alpaca_bars(
            symbol=symbol,
            unit=unit,
            bar_size=bar_size,
            indicators=indicators,
            bars_back=plot_bar_count,
            truncate_bars=False,
            include_outside_hours=include_outside_hours,
        )),
        parse_dates=True,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    bars_df["datetime"] = pd.to_datetime(bars_df["datetime"])
    bars_df.set_index("datetime", inplace=True)
    total_time_span = (bars_df.index[-1] - bars_df.iloc[-plot_bar_count:].index[0]).total_seconds() / 60 / 60
    time_span_str = f"{int(math.ceil(total_time_span))} hours"
    if unit == "Daily":
        time_span_str = f"{int(math.ceil(total_time_span / 24))} days"
    elif unit == "Weekly":
        time_span_str = f"{int(math.ceil(total_time_span / 24 / 7))} weeks"
    elif unit == "Monthly":
        time_span_str = f"{int(math.ceil(total_time_span / 24 / 30))} months"

    # Generate the plot
    buf = plot_bars(
        bars_df.iloc[-plot_bar_count:],
        f"{symbol}\nbar size: {bar_size}\nbar unit: {unit}\ntotal time span: {time_span_str}",
    )

    bars_df.reset_index(inplace=True)
    bars_df["datetime"] = bars_df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Return both the image and the data
    return (
        Image(data=buf.read(), format="png"),
        bars_df.iloc[-(bars_back or plot_bar_count):].to_csv(index=False),
    )
    

async def get_most_recent_bar(
    symbol_list: list[str] = Field(..., description="Symbols to get the most recent bar for"),
    bar_size: int = Field(..., description="The size of the bar to get."),
    bar_unit: str = Field(..., description="The unit of the bar to get. Possible values are Minute, Hour, Daily, Weekly, Monthly."),
    include_outside_hours: bool = Field(default=False, description="Whether to include data from pre-market and post-market sessions (only applies to minute and hourly bars)"),
) -> str:
    """
    Get the most recent OHLCV bar for given symbols, bar size, and bar unit.
    Example:
        symbols: ["AAPL", "MSFT"]
        bar_size: 1
        bar_unit: "Hour"

        Returns:
        {
            "AAPL": {
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "volume": 10000,
            },
            "MSFT": {
                "open": 200,
                "high": 210,
                "low": 190,
                "close": 205,
                "volume": 20000,
            }
        }
    """
    dfs = await asyncio.gather(*[
        get_alpaca_bars(symbol, bar_unit, 1, bar_size, indicators="", truncate_bars=False, include_outside_hours=include_outside_hours) for symbol in symbol_list
    ])
    bars = {}
    for df, symbol in zip(dfs, symbol_list):
        parsed = pd.read_csv(StringIO(df))
        ohlcv = parsed.iloc[-1].to_dict()
        bars[symbol] = ohlcv

    return json.dumps(bars)
