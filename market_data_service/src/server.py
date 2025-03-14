from dotenv import load_dotenv

load_dotenv()
import logging
from alpaca_api.news import get_news, latest_headline_resource
from alpaca_api.market_data import (
    SUPPORTED_INDICATORS,
    get_alpaca_bars as get_bars,
    plot_alpaca_bars_with_indicators as plot_bars_with_indicators,
    get_most_recent_bar
)
# from options import get_option_chain, get_option_expirations
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("market-data-service")

# Create main FastMCP server
mcp = FastMCP(name="Market Data Service")

# Add tools
mcp.add_tool(
    get_bars,
    name="get_bars"
)

mcp.add_tool(
    plot_bars_with_indicators,
    name="plot_bars_with_indicators",
)
mcp.add_tool(get_news)
mcp._resource_manager._templates[latest_headline_resource.uri_template] = (
    latest_headline_resource
)
mcp.add_tool(get_most_recent_bar)
# mcp.add_tool(get_option_chain)
# mcp.add_tool(get_option_expirations)
    

def main():
    mcp.run(transport="sse")


if __name__ == "__main__":
    main()
