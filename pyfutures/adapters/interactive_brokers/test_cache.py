from pyfutures.adapters.interactive_brokers.cache import HistoricCache
from pyfutures.adapters.interactive_brokers.cache import RequestBarsCache

def test_purge_cache():
    request_bars = RequestBarsCache(
        client=self._client,
        name="request_bars",
        timeout_seconds=60 * 10,
    )
    