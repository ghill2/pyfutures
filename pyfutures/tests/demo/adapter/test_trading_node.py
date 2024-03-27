import asyncio
import logging

from nautilus_trader.model.identifiers import TraderId
from pyfutures.logger import LoggerAttributes
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.unit.adapter.stubs import AdapterStubs
from pyfutures.tests.unit.client.stubs import ClientStubs

from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from pytower.tests.stubs.strategies import BuyOnBarX
from pyfutures.adapter.parsing import AdapterParser


async def main_universe():
    """
    Instantiating a trading node per universe
    started using forex instead of futures to debug data subscriptions
    """
    rows = IBTestProviderStubs.universe_rows()
    loop = asyncio.get_event_loop()

    # this is a workaround to get the instrument id of the current front contract
    # as the node doesnt support loading instrument_ids without a month
    client = ClientStubs.uncached_client(client_id=2, loop=loop)
    await client.connect()

    tasks = []
    for row in rows[42:43]:
        details = await client.request_front_contract_details(row.contract_cont)
        details.contract.secType = "FUT"
        instrument_id = AdapterParser.details_to_instrument_id(details)
        # break
        # TODO: george: what about creating a cache to store all contract details for all universe instruments, create a BaseCache with different variations
        LoggerAttributes.level = logging.DEBUG
        LoggerAttributes.id = row.trading_class
        node = AdapterStubs.trading_node(loop=loop, trader_id=TraderId("TRADER-001"), load_ids=[instrument_id.value])

        # bar_type = BarType.from_str("EUR.GBP=CASH.IDEALPRO-5-SECOND-BID-EXTERNAL")
        strategy = BuyOnBarX(
            index=1,
            bar_type=BarType.from_str(f"{instrument_id.value}-5-SECOND-BID-EXTERNAL"),
            order_side=OrderSide.BUY,
            quantity=1,
        )
        node.trader.add_strategy(strategy)

        tasks.append(loop.create_task(node.run_async()))
    # check if task was running
    # check if all instruments specified were loaded into the provider

    await asyncio.gather(*tasks)


async def main():
    loop = asyncio.get_event_loop()
    LoggerAttributes.level = logging.DEBUG
    LoggerAttributes.id = "EURGBP"
    instrument_id_str = "EUR.GBP=CASH.IDEALPRO"
    node = AdapterStubs.trading_node(loop=loop, trader_id=TraderId("TRADER-001"), load_ids=[instrument_id_str])

    strategy = BuyOnBarX(
        index=1,
        bar_type=BarType.from_str(f"{instrument_id_str}-5-SECOND-BID-EXTERNAL"),
        order_side=OrderSide.BUY,
        quantity=1,
    )
    node.trader.add_strategy(strategy)

    await loop.create_task(node.run_async())


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
