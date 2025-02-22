from datetime import datetime, timedelta
from logging import getLogger

import pytz
# Alpaca imports:
from alpaca.trading.enums import OrderStatus, OrderType, QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest
# Assume you’re using the same resource pattern as before:
from mcp.server.fastmcp.resources import FunctionResource, ResourceTemplate

from common_lib.alpaca_helpers.async_impl.trading_client import AsyncTradingClient
# If you have your Alpaca settings in a helper class:
from common_lib.alpaca_helpers.env import AlpacaSettings
from common_lib.alpaca_helpers.simulation.simulation_trading_db import SimulationTradingDatabase
from common_lib.alpaca_helpers.simulation.simulation_trading_client import SimulationTradingClient

logger = getLogger(__name__)

# Initialize your trading client
settings = AlpacaSettings()

if settings.simulation:
    simulation_trading_database: SimulationTradingDatabase = SimulationTradingDatabase(settings.simulation_database_url)
    trading_client = SimulationTradingClient(simulation_trading_database)
else:
    trading_client = AsyncTradingClient(settings.api_key, settings.api_secret)


async def get_portfolio(symbol: str) -> str:
    """
    Get account portfolio holdings, including stocks (and possibly crypto if enabled).
    Returns a nicely formatted multiline string.
    """
    positions = await trading_client.get_all_positions()

    lines = []
    for pos in positions:
        if pos.symbol == symbol or symbol == "all":
            lines.append(
                f"Symbol: {pos.symbol}, "
                f"Size: {pos.qty}, "
                f"Avg Entry Price: {pos.avg_entry_price}, "
                f"Market Value: {pos.market_value}, "
                f"Unrealized P/L: {float(pos.unrealized_pl):.2f}, "
                f"Unrealized P/L %: {float(pos.unrealized_plpc):.2%}, "
                f"Side: {pos.side}, "
                f"Current Price: {pos.current_price}, "
            )
    if not lines:
        return f"No positions found for {symbol}."

    return "".join(lines)


portfolio_resource = ResourceTemplate(
    uri_template="account://portfolio/{symbol}",
    name="Get account portfolio holdings",
    description="Get account portfolio holdings (stocks, etc.)",
    fn=get_portfolio,
    parameters={
        "symbol": {
            "type": "string",
            "description": "The symbol of the portfolio to get holdings for",
            "default": "all"
        }
    }
)


async def get_account_summary() -> str:
    """
    Get high-level account information, like buying power, equity, etc.,
    returned in a simple multiline string.
    """
    account = await trading_client.get_account()  # this is done sync because mcp bug where a resources cant be async (?)
    lines = [
        "Account Summary:",
        "----------------",
        f"Account ID: {account.id}",
        f"Account Number: {account.account_number}",
        f"Account Status: {account.status}",
        f"Buying Power: {account.buying_power}",
        f"Equity: {account.equity}",
        f"Portfolio Value: {account.portfolio_value}",
        f"Currency: {account.currency}",
        f"Maintenance Margin: {account.maintenance_margin}",
    ]
    return "\n".join(lines)


account_summary_resource = FunctionResource(
    uri="account://account_summary",
    name="Get account summary information",
    description="Get high-level account info such as buying power, equity, etc.",
    fn=get_account_summary,
)


async def get_completed_orders(symbol: str) -> str:
    orders = await trading_client.get_orders(filter=GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=datetime.now() - timedelta(days=1),
        symbols=[symbol]
    ))
    lines = ["\n"]
    for o in orders:
        if o.status in [OrderStatus.FILLED, OrderStatus.HELD]:
            lines.append(
                f"<order>"
                f"<id>{o.id}</id>"
                f"<symbol>{o.symbol}</symbol>"
                f"<side>{o.side}</side>"
                f"<qty>{o.qty}</qty>"
                f"<type>{o.type}</type>"
                f"<status>{o.status}</status>"
                f"<filled_qty>{o.filled_qty}</filled_qty>"
                f"<filled_avg_price>{o.filled_avg_price}</filled_avg_price>"
                f"<filled_at>{o.filled_at.astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S') if o.filled_at else 'N/A'}</filled_at>"
                f"<position_intent>{o.position_intent.value if o.position_intent else 'N/A'}</position_intent>"
                f"</order>"
            )
    return "\n".join(lines)


completed_orders_resource = ResourceTemplate(
    uri_template="brokerage://completed_orders/{symbol}",
    name="Get all orders in the account from the current session",
    description="Get all orders in the account from the current session (excluding canceled).",
    fn=get_completed_orders,
    parameters={
        "symbol": {
            "type": "string",
            "description": "The symbol of the orders to get",
            "default": "all"
        }
    }
)


async def get_open_orders(symbol: str) -> str:
    """
    Get only open orders in the account and return them in a multiline string.
    """
    open_orders = await trading_client.get_orders(
        filter=GetOrdersRequest(
            status=QueryOrderStatus.OPEN,
            symbols=[symbol]
        )
    )
    if not open_orders:
        return "No open orders found."

    lines = []
    for o in open_orders:
        line = (
            f"<order>"
            f"<id>{o.id}</id>"
            f"<symbol>{o.symbol}</symbol>"
            f"<side>{o.side}</side>"
            f"<qty>{o.qty}</qty>"
            f"<status>{o.status}</status>"
            f"<type>{o.type}</type>"
            f"<price>{o.limit_price if o.type == OrderType.LIMIT else o.stop_price if o.type in [OrderType.STOP, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP] else 'N/A'}</price>"
            f"<position_intent>{o.position_intent.value if o.position_intent else 'N/A'}</position_intent>"
            f"<created_at>{o.created_at.astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S') if o.created_at else 'N/A'}</created_at>"
            f"</order>"
        )
        lines.append(line)
    return "\n".join(lines)


open_orders_resource = ResourceTemplate(
    uri_template="brokerage://open_orders/{symbol}",
    name="Get all open orders in the account",
    description="Get all open orders in the account",
    fn=get_open_orders,
    parameters={
        "symbol": {
            "type": "string",
            "description": "The symbol of the orders to get",
            "default": "all"
        }
    }
)


async def has_order_filled(order_id: str) -> bool:
    order = await trading_client.get_order_by_id(order_id)
    return order.filled_qty == order.qty


order_filled_resource = ResourceTemplate(
    uri_template="brokerage://order_filled/{order_id}",
    name="Check if an order has been filled",
    description="Check if an order has been filled",
    fn=has_order_filled,
    parameters={
        "order_id": {
            "type": "string",
            "description": "The ID of the order to check",
        }
    }
)
