import pytest
from pyfutures.stats.stats import Stats
import asyncio


def test_stats(client):
    stats = Stats(client=client)
    # stats._get_contract_price(exchange="CME", symbol="DC", url="https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=668447027")
    # stats.calc()
    stats.test_last_close()
