"""
Constants for the project.
"""
import pytz

EASTERN_TZ = pytz.timezone("America/New_York")

ISHARES_ETF_URLS = {
    "IWM": (
        "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/"
        "1467271812596.ajax?fileType=csv&fileName=IWM&dataType=fund"
    ),
}

# The vBase forwarder URL for making commitments via the vBase forwarder.
VBASE_FORWARDER_URL = "https://dev.api.vbase.com/forwarder-test/"

# The name of the portfolio set (collection).
# This is the vBase set (collection) that receive the object commitments (stamps)
# for the individual portfolios.
PORTFOLIO_NAME = "TestPortfolio"