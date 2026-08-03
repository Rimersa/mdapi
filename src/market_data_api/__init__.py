"""Five-minute market-data API.

The remote gateway only reads published derived objects.  Original lake files
are never opened for writing by this package.
"""

from .model import DataRequest, FetchMode, UpdateMode
from .sdk import MarketDataAPIError, MarketDataClient
from .service import DataService, ServiceLimits

__all__ = [
    "DataRequest",
    "DataService",
    "FetchMode",
    "MarketDataAPIError",
    "MarketDataClient",
    "ServiceLimits",
    "UpdateMode",
]
__version__ = "0.4.0"
