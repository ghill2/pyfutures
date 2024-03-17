import asyncio
import functools
import logging
import queue
import time
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import eventkit

import traceback
import pandas as pd
from ibapi import comm
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.common import BarData
from ibapi.common import HistoricalTickBidAsk
from ibapi.common import HistoricalTickLast
from ibapi.common import ListOfHistoricalSessions
from ibapi.common import ListOfHistoricalTickBidAsk
from ibapi.common import ListOfHistoricalTickLast
from ibapi.common import OrderId
from ibapi.common import TickAttribBidAsk
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.decoder import Decoder
from ibapi.execution import Execution as IBExecution
from ibapi.execution import ExecutionFilter
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from ibapi.wrapper import EWrapper

from pyfutures.client.cache import RequestsCache
from pyfutures.client.cache import DetailsCache
from pyfutures.client.cache import CachedFunc
from pyfutures.client.connection import Connection
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import Duration
from pyfutures.client.enums import WhatToShow
from pyfutures.client.objects import ClientException
from pyfutures.client.objects import ClientRequest
from pyfutures.client.objects import ClientSubscription
from pyfutures.client.objects import IBErrorEvent
from pyfutures.client.objects import IBExecutionEvent
from pyfutures.client.objects import IBOpenOrderEvent
from pyfutures.client.objects import IBOrderStatusEvent
from pyfutures.client.objects import IBPortfolioEvent
from pyfutures.client.objects import IBPositionEvent
from pyfutures.client.parsing import ClientParser
from pyfutures.logger import LoggerAdapter


class InteractiveBrokersClient(EWrapper):
    _request_id_map = {
        # position request id is reserve for order
        # -1 reserve for no id from IB
        "next_order_id": -3,
        "positions": -4,
        "accounts": -5,
        "portfolio": -6,
        "orders": -7,
        # request id below 10 is where the decrementing sequence starts
    }

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
        api_log_level: int = logging.ERROR,
        request_timeout_seconds: float | int | None = None,  # default timeout for requests if not given
        override_timeout: bool = False,  # overrides timeout for all request even if given, useful for testing
    ):
        # Events
        self.order_status_events = eventkit.Event("IBOrderStatusEvent")
        self.open_order_events = eventkit.Event("IBOpenOrderEvent")
        self.error_events = eventkit.Event("IBErrorEvent")
        self.execution_events = eventkit.Event("IBExecutionEvent")
        self._log = LoggerAdapter.from_name(name=type(self).__name__)

        # Config
        self._loop = loop
        self._requests = {}
        self._subscriptions = {}
        self._executions = {}  # hot cache

        names = logging.Logger.manager.loggerDict
        for name in names:
            if "ibapi" in name:
                logging.getLogger(name).setLevel(api_log_level)

        self._request_timeout_seconds = request_timeout_seconds
        self._override_timeout = override_timeout
        if override_timeout:
            assert isinstance(self._request_timeout_seconds, (float, int))

        self._connection = Connection(loop=loop, host=host, port=port, client_id=client_id, subscriptions=self._subscriptions)
        self._connection.register_handler(self._handle_msg)

        self._eclient = EClient(wrapper=None)
        # not using eclient socket so always True to pass all messages
        self._eclient.isConnected = lambda: True
        self._eclient.serverVersion = lambda: 176
        self._eclient.conn = self  # where to send the messages: eclient -> sendMsg
        self._eclient.clientId = client_id

        self._decoder = Decoder(wrapper=self, serverVersion=176)

        self._outgoing_msg_task: asyncio.Task | None = None
        self._outgoing_msg_queue = queue.Queue()

        self._reset()
        self._parser = ClientParser()

    @property
    def subscriptions(self) -> list[ClientSubscription]:
        return self._subscriptions.values()

    @property
    def requests(self) -> list[ClientRequest]:
        return self._requests.values()

    @property
    def connection(self) -> Connection:
        return self._connection

    ################################################################################################
    # Connection

    async def connect(self, timeout_seconds: int = 5) -> None:
        self._outgoing_msg_task = self._loop.create_task(
            coro=self._process_outgoing_msg_queue(),
            name="outgoing_message_queue",
        )

        await self._connection.connect(timeout_seconds=timeout_seconds)

    def sendMsg(self, msg: bytes) -> None:
        # messages output from self.eclient are sent here
        self._outgoing_msg_queue.put(msg)

    async def _process_outgoing_msg_queue(self) -> None:
        while True:
            if self.connection.is_connected:
                while not self._outgoing_msg_queue.empty():
                    msg: bytes = self._outgoing_msg_queue.get()
                    self._connection.sendMsg(msg)
                await asyncio.sleep(0)
            else:
                self._log.debug("Stopping outgoing messages, the client is disconnected. Waiting for 5 seconds...")
                await asyncio.sleep(5)

    def _handle_msg(self, msg: bytes) -> None:
        fields = comm.read_fields(msg)
        try:
            self._decoder.interpret(fields)
        except Exception as e:
            self._log.exception("_listen callback exception, _listen task still running...", e)

    def _reset(self) -> None:
        self._request_id_seq = -10

        if self._outgoing_msg_task is not None:
            self._outgoing_msg_task.cancel()
        self._outgoing_msg_task = None

        self._connection._reset()

    def _stop(self) -> None:
        self._reset()

    ################################################################################################
    # Responses

    def _next_request_id(self) -> int:
        current = self._request_id_seq
        self._request_id_seq -= 1
        return current

    def _create_request(
        self,
        id: int,
        data: list | dict | None = None,
        timeout_seconds: int | None = None,
    ) -> ClientRequest:
        assert isinstance(id, int)

        if self._override_timeout:
            timeout_seconds = self._request_timeout_seconds
        else:
            timeout_seconds = timeout_seconds or self._request_timeout_seconds

        request = ClientRequest(
            id=id,
            data=data,
            timeout_seconds=timeout_seconds,
        )

        self._requests[id] = request

        return request

    async def _wait_for_request(self, request: ClientRequest) -> Any:
        try:
            await asyncio.wait_for(request, timeout=request.timeout_seconds)
        except asyncio.TimeoutError:
            del self._requests[request.id]
            raise

        result = request.result()

        del self._requests[request.id]

        if isinstance(result, ClientException):
            self._log.error(result.message)
            raise result

        return result

    ################################################################################################
    # Error

    def error(
        self,
        reqId: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ) -> None:  # : Override the EWrapper
        # TODO: if reqId is negative, its part of a request

        event = IBErrorEvent(
            reqId=reqId,
            errorCode=errorCode,
            errorString=errorString,
            advancedOrderRejectJson=advancedOrderRejectJson,
        )
        self.error_events.emit(event)

        # Note: -1 will indicate a notification and not true error condition
        if reqId == -1:
            # self._log.debug(f"Notification {errorCode}: {errorString}")
            return
        elif "warning" in errorString.lower():
            # 2121: Warning: 2 products are trading on the basis other than currency price
            # disallow warnings to set the result of a pending request
            self._log.warning(f"{errorCode}: {errorString}")
            return

        request = self._requests.get(reqId)
        if request is None:
            self.error_events.emit(event)
            return  # no response found for request_id

        exception = ClientException(code=errorCode, message=errorString)

        request.set_result(exception)  # set_exception does not stop awaiting?

    ################################################################################################
    # Market Data Type

    async def request_market_data_type(self, market_data_type: int):
        """
        by default only real-time (1) market data is enabled sending
           1 (real-time) disables frozen, delayed and delayed-frozen market data sending
           2 (frozen) enables frozen market data sending
           3 (delayed) enables delayed and disables delayed-frozen market data sending
           4 (delayed-frozen) enables delayed and delayed-frozen market data
        """
        self._eclient.reqMarketDataType(marketDataType=market_data_type)
        await asyncio.sleep(1)  # no reliable way to confirm type has been changed

    ################################################################################################
    # reqMktData

    async def request_market_data(
        self,
        ticker_id: int,
        contract: IBContract,
        generic_tick_list: list[int],
    ):
        await self._eclient.reqMktData(
            tickerId=ticker_id,
            contract=contract,
            genericTickList=generic_tick_list,
            snapshot=True,
            regulatorySnaphsot=False,
            mktDataOptions=[],
        )

    ################################################################################################
    # Order Execution

    def place_order(self, order: IBOrder) -> None:
        self._eclient.placeOrder(order.orderId, order.contract, order)

    def cancel_order(self, order_id: int) -> None:
        self._eclient.cancelOrder(orderId=order_id, manualCancelOrderTime="")

    ################################################################################################
    # Contract Details
    async def request_contract_details(
        self,
        contract: IBContract,
        cache: DetailsCache | Path | None = None,
    ):
        func: Callable = self._request_contract_details

        if cache is not None:
            if isinstance(cache, Path):
                cache = DetailsCache(cache)
            func: Callable = CachedFunc(
                func=func,
                cache=cache,
            )

        details: list[IBContractDetails] = await func(contract=contract)
        return details

    async def _request_contract_details(self, contract: IBContract) -> list[IBContractDetails]:
        self._log.debug(f"Requesting contract details for {contract=}")

        request = self._create_request(
            id=self._next_request_id(),
            data=[],
        )

        self._eclient.reqContractDetails(reqId=request.id, contract=contract)

        return await self._wait_for_request(request)

    async def request_last_contract_month(self, contract: IBContract) -> str:
        self._log.debug(
            f"Requesting last contract month for: {contract.symbol}, {contract.tradingClass}",
        )

        details_list = await self.request_contract_details(contract)

        return details_list[-1].contractMonth

    async def request_front_contract_details(self, contract: IBContract) -> IBContractDetails | None:
        self._log.debug(
            f"Requesting front contract for: {contract.symbol}, {contract.tradingClass}",
        )

        details_list = await self.request_contract_details(contract)

        if len(details_list) == 0:
            return None

        return details_list[0]

    async def request_front_contract(self, contract: IBContract) -> IBContract:
        details: IBContractDetails = await self.request_front_contract_details(contract)
        return details.contract

    def contractDetails(
        self,
        reqId: int,
        contractDetails: IBContractDetails,
    ):
        self._log.debug("contractDetails")

        request = self._requests.get(reqId)

        # self._log.debug(repr(request))

        if request is None:
            self._log.error(f"No request found for {reqId}")
            return

        request.data.append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        self._log.debug("contractDetailsEnd")

        request = self._requests.get(reqId)
        if request is None:
            self._log.error(f"No request found for {reqId}")
            return

        request.set_result(sorted(request.data, key=lambda x: x.contractMonth))

    ################################################################################################
    # Request bars
    async def request_last_bar(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
    ) -> BarData | None:
        bars = await self.request_bars(
            contract=contract,
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=bar_size.to_appropriate_duration(),
            end_time=pd.Timestamp.utcnow(),
        )
        return bars[-1] if len(bars) > 0 else None

    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
        cache: RequestsCache | Path | None = None,
        delay: float = 0,
        as_dataframe: bool = False,
    ):
        # TODO: do not cache time range that might have missing data
        # if end_time >= pd.Timestamp.utcnow():
        #     cache = None
        # else:
        #     cache = self._cache

        kwargs = dict(
            contract=contract,
            bar_size=bar_size,
            what_to_show=what_to_show,
            duration=duration,
            end_time=end_time,
        )

        # initialize cache
        func: Callable = self._request_bars
        if cache is None:
            is_cached = False
        else:
            if isinstance(cache, Path):
                cache = RequestsCache(cache)
            func: Callable = CachedFunc(
                func=func,
                cache=cache,
            )
            is_cached = func.is_cached(**kwargs)

        # request bars
        start = time.perf_counter()
        bars = []
        try:
            bars: list[BarData] = await func(**kwargs)
        except ClientException:
            pass
        except asyncio.TimeoutError as e:
            self._log.error(str(e.__class__.__name__))
        stop = time.perf_counter()

        if is_cached:
            self._log.info(f"Read {len(bars)} from cache")
        else:
            self._log.info(f"Elapsed time: {stop - start:.1f}s")

        # delay if needed
        if delay > 0 and not is_cached:
            self._log.info(f"Waiting for {delay}s...")
            await asyncio.sleep(delay)

        if as_dataframe:
            return pd.DataFrame([self._parser.bar_data_to_dict(obj) for obj in bars])

        return bars

    async def _request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp,
    ) -> list[BarData]:
        """
        formatDate=1, returns timestamp in the exchange timezone
        formatDate=2, returns timestamp as integer seconds from epoch (UTC)
        """
        start_time = end_time - duration.to_timedelta()
        self._log.info(f"{start_time} | {end_time} | {bar_size} | {duration} | {what_to_show} | {contract}")

        request: ClientRequest = self._create_request(
            id=self._next_request_id(),
            data=[],
            timeout_seconds=60 * 10,
        )

        try:
            self._eclient.reqHistoricalData(
                reqId=request.id,
                contract=contract,
                endDateTime=end_time.tz_convert("UTC").strftime(format="%Y%m%d-%H:%M:%S"),
                durationStr=str(duration),
                barSizeSetting=str(bar_size),
                whatToShow=what_to_show.name,
                useRTH=1,
                formatDate=2,
                keepUpToDate=False,
                chartOptions=[],
            )
            bars = await self._wait_for_request(request)
        except ClientException:
            self._eclient.cancelHistoricalData(reqId=request.id)
            raise

        if len(bars) > 0:
            self._log.info(f"---> Downloaded {len(bars)} bars. {bars[0].timestamp} {bars[-1].timestamp}")
        else:
            self._log.info("---> Downloaded 0 bars.")

        previous_len = len(bars)

        bars = [b for b in bars if b.timestamp >= start_time and b.timestamp < end_time]

        if previous_len != len(bars):
            filtered_count = previous_len - len(bars)
            self._log.info(f"---> Filtered {filtered_count} bars.")

        return bars

    def historicalData(self, reqId: int, bar: BarData):  # : Override the EWrapper
        request = self._requests.get(reqId)
        if request is None:
            return  # no request found for request_id

        bar.timestamp = self._parser.parse_datetime(bar.date)
        request.data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):  # : Override the EWrapper
        request = self._requests.get(reqId)
        if request is None:
            return  # no request found for request_id
        request.set_result(request.data)

    ################################################################################################
    # Request order id

    async def request_next_order_id(
        self,
    ) -> int:
        request_id = self._request_id_map["next_order_id"]
        request = self._create_request(request_id)

        self._eclient.reqIds(1)

        return await self._wait_for_request(request)

    def nextValidId(self, orderId: int):
        self._log.debug(f"nextValidId {orderId}")

        request_id = self._request_id_map["next_order_id"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug("no orders response, returning...")
            return  # no response found for request_id

        request.set_result(orderId)

    ################################################################################################
    # Order querying

    async def request_open_orders(self) -> list[IBOpenOrderEvent]:
        """
        Call this function to request the open orders that were placed from this client.

        Each open order will be fed back through the openOrder() and orderStatus()
        functions on the EWrapper.
        """
        request_id = self._request_id_map["orders"]
        request = self._create_request(id=request_id, data=[])

        self._eclient.reqOpenOrders()

        return await self._wait_for_request(request)

    async def request_completed_orders(self) -> list[IBOpenOrderEvent]:
        """
        TODO: for trade reporting
        self._eclient.reqCompletedOrders
        """

    def orderStatus(
        self,
        orderId: OrderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:  # : Override the EWrapper
        """
        This event is called whenever the status of an order changes.

        It is also fired after reconnecting to TWS if the client has any open orders.

        """
        self._log.debug(f"orderStatus {status}")

        request_id = self._request_id_map["orders"]
        request = self._requests.get(request_id)
        if request is not None:
            self._log.debug("blocking event: client is currently requesting orders")
            return

        event = IBOrderStatusEvent(
            orderId=orderId,
            status=status,
            filled=filled,
            remaining=remaining,
            avgFillPrice=avgFillPrice,
            permId=permId,
            parentId=parentId,
            lastFillPrice=lastFillPrice,
            clientId=clientId,
            whyHeld=whyHeld,
            mktCapPrice=mktCapPrice,
        )

        self.order_status_events.emit(event)

    def openOrder(
        self,
        orderId: OrderId,
        contract: IBContract,
        order: IBOrder,
        orderState: IBOrderState,
    ) -> None:  # : Override the EWrapper
        """
        self._client.reqOpenOrders callback This function is called to feed in open
        orders.

        orderID: OrderId - The order ID assigned by TWS. Use to cancel or
            update TWS order.
        contract: Contract - The Contract class attributes describe the contract.
        order: Order - The Order class gives the details of the open order.
        orderState: OrderState - The orderState class includes attributes Used
            for both pre and post trade margin and commission data.

        The IBApi.EWrapper.openOrder method delivers an IBApi.Order object representing an
        open order within the system.

        In addition, IBApi.EWrapper.openOrder returns an an IBApi.OrderState object that is used
        to return estimated pre-trade margin and commission information in response
        to invoking IBApi.EClient.placeOrderwith a IBApi.Order object that has the flag
        IBApi.Order.WhatIf flag set to True

        OrderStatusReport

        """
        self._log.info(
            f"openOrder {orderId}, orderStatus {orderState.status}, commission: {orderState.commission}{orderState.commissionCurrency}, completedStatus: {orderState.completedStatus}"
        )

        if orderState.warningText != "":
            self._log.warning(f"order {orderId} has warning: {orderState.warningText}")

        event = IBOpenOrderEvent(
            contract=contract,
            order=order,
            orderState=orderState,
        )

        request_id = self._request_id_map["orders"]
        request = self._requests.get(request_id)
        if request is None:
            self.open_order_events.emit(event)
            return

        request.data.append(event)

    def openOrderEnd(self):
        """
        self._client.reqOpenOrders callback.
        """
        self._log.info("openOrderEnd")

        request_id = self._request_id_map["orders"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug(f"No request found for id {request_id}")
            return

        request.set_result(request.data)

    ################################################################################################
    # Positions query

    async def request_positions(self) -> list[IBPositionEvent]:
        request_id = self._request_id_map["positions"]
        request = self._create_request(
            id=request_id,
            data=[],
        )

        self._eclient.reqPositions()

        return await self._wait_for_request(request)

    def position(  # : Override the EWrapper
        self,
        account: str,
        contract: IBContract,
        position: Decimal,
        avgCost: float,
    ) -> None:
        """
        This event returns real-time positions for all accounts in response to the
        reqPositions() method.
        """
        self._log.info("position")

        request_id = self._request_id_map["positions"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug("no request for positions... returning")
            return  # no response found for request_id

        request.data.append(
            IBPositionEvent(
                account=account,
                conId=contract.conId,
                quantity=position,
                avgCost=avgCost,
            ),
        )

    def positionEnd(self):
        """
        This is called once all position data for a given request are received and
        functions as an end marker for the position() data.
        """
        self._log.info("positionEnd")

        request_id = self._request_id_map["positions"]
        request = self._requests.get(request_id)
        if request is None:
            return  # no response found for request_id

        request.set_result(request.data)

    ################################################################################################
    # Executions query

    async def request_executions(self, client_id: int):
        request_id: int = self._next_request_id()
        request = self._create_request(
            id=request_id,
            data=[],
        )

        filter = ExecutionFilter()
        filter.client_id = client_id

        self._eclient.reqExecutions(
            reqId=request.id,
            execFilter=filter,
        )

        return await self._wait_for_request(request)

    def execDetails(
        self,
        reqId: int,
        contract: IBContract,
        execution: IBExecution,
    ):  # : Override the EWrapper
        """
        This event is fired when the reqExecutions() functions is invoked, or when an
        order is filled.

        NOTE: when an order is filled, the execDetails and commissionReport endpoint is called
        The order is execDetails -> commissionReport

        NOTE: the reqId is -1 for events
        """
        self._log.info(f"execDetails reqId={reqId} {execution}")

        event = IBExecutionEvent(
            timestamp=self._parser.parse_datetime(execution.time),
            reqId=reqId,
            contract=contract,
            execution=execution,
            commissionReport=None,  # filled on commissionReport callback
        )

        self._executions[execution.execId] = event  # hot cache

    def commissionReport(self, commissionReport: IBCommissionReport):  # : Override the EWrapper
        """
        The commissionReport() callback is triggered as follows:

        - immediately after a trade execution
        - by calling reqExecutions().

        NOTE: when an order is filled, the execDetails and commissionReport endpoint is called
        The order is execDetails -> commissionReport
        """
        self._log.info(f"commissionReport {commissionReport}")

        # find execution from hot cache
        exec_id = commissionReport.execId

        event = self._executions.get(exec_id)  # hot cache

        if event is None:
            self._log.debug(f"No execution event found in hot cache for {exec_id}")
            return

        del self._executions[exec_id]

        event.commissionReport = commissionReport

        # event based
        if event.reqId == -1:
            self.execution_events.emit(event)
            return

        # request based
        request = self._requests.get(event.reqId)
        if request is None:
            self._log.debug(f"No request found for request_id {event.reqId}")
            return

        request.data.append(event)

    def execDetailsEnd(self, reqId: int):  # : Override the EWrapper
        """
        This function is called once all executions have been sent to a client in
        response to reqExecutions().
        """
        self._log.info("execDetailsEnd")

        request = self._requests.get(reqId)
        if request is None:
            self._log.debug(f"No request found for request_id {reqId}")
            return

        request.set_result(request.data)

    ################################################################################################
    # Account and Portfolio
    async def request_account_summary(self) -> dict:
        """
        Call this method to request and keep up to date the data that appears on the TWS
        Account Window Summary tab.

        The data is returned by accountSummary().

        """
        request = self._create_request(
            id=self._next_request_id(),
            data={},
        )

        self._eclient.reqAccountSummary(
            reqId=request.id,
            groupName="All",
            tags=AccountSummaryTags.AllTags,
        )

        return await self._wait_for_request(request)

    def accountSummary(
        self,
        reqId: int,
        account: str,
        tag: str,
        value: str,
        currency: str,
    ):  # : Override the EWrapper
        """
        Returns the data from the TWS Account Window Summary tab in response to
        reqAccountSummary().
        """
        self._log.debug("accountSummary")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        if request.data.get("account") is not None:
            assert request.data["account"] == account  # multiple accounts not support yet

        request.data["account"] = account
        request.data["currency"] = currency
        request.data[tag] = value

    def accountSummaryEnd(self, reqId: int):
        """
        This method is called once all account summary data for a given request are
        received.
        """
        self._log.debug("accountSummaryEnd")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        request.set_result(request.data)

    ################################################################################################
    # Head timestamp

    async def request_head_timestamp(
        self,
        contract: IBContract,
        what_to_show: WhatToShow,
        use_rth: bool = True,
    ) -> pd.Timestamp | None:
        self._log.debug(
            f"Requesting head timestamp for {contract.symbol} {contract.exchange} {contract.conId}",
        )
        request = self._create_request(
            id=self._next_request_id(),
        )

        try:
            self._eclient.reqHeadTimeStamp(
                reqId=request.id,
                contract=contract,
                whatToShow=what_to_show.name,
                useRTH=use_rth,
                formatDate=1,
            )

            return await self._wait_for_request(request)

        except ClientException as e:
            if not e.message.endswith("No head time stamp"):
                raise e

        # 162: Historical Market Data Service error message:No head time stamp
        return None

    def headTimestamp(self, reqId: int, headTimestamp: str):
        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        head_timestamp: pd.Timestamp = pd.to_datetime(headTimestamp, format="%Y%m%d-%H:%M:%S", utc=True)
        request.set_result(head_timestamp)

    ################################################################################################
    # Request quote ticks

    async def request_last_quote_tick(
        self,
        contract: IBContract,
    ) -> HistoricalTickBidAsk:
        self._log.debug(f"Requesting last quote tick for {contract.symbol}")
        quotes = await self.request_quote_ticks(
            contract=contract,
            count=1,
            start_time=pd.Timestamp.utcnow() - pd.Timedelta(days=365),
            end_time=pd.Timestamp.utcnow(),
        )
        return None if len(quotes) == 0 else quotes[-1]

    async def request_first_quote_tick(
        self,
        contract: IBContract,
        use_rth: bool = True,
    ) -> HistoricalTickBidAsk | None:
        self._log.debug(f"Requesting last quote tick for {contract.symbol}")
        head_timestamp = await self.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID_ASK,
            use_rth=use_rth,
        )
        self._log.debug(f"--> req_head_timestamp: {head_timestamp}")
        quotes = await self.request_quote_ticks(
            contract=contract,
            count=1,
            start_time=head_timestamp,
            use_rth=use_rth,
        )
        return None if len(quotes) == 0 else quotes[0]

    async def request_quote_ticks(
        self,
        contract: IBContract,
        # start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        count: int = 1000,
    ) -> list[HistoricalTickBidAsk]:
        """
        End Date/Time: The date, time, or time-zone entered is invalid.
        The correct format is yyyymmdd hh:mm:ss xx/xxxx where yyyymmdd and xx/xxxx are optional.
        E.g.: 20031126 15:59:00 US/Eastern
        Note that there is a space between the date and time, and between the time and time-zone.
        If no date is specified, current date is assumed.
        If no time-zone is specified, local time-zone is assumed(deprecated).
        You can also provide yyyymmddd-hh:mm:ss time is in UTC.
        Note that there is a dash between the date and time in UTC notation.
        """
        # assert start_time.tz is not None, "Timestamp is not timezone aware"
        assert end_time.tz is not None, "Timestamp is not timezone aware"
        # assert start_time < end_time

        request = self._create_request(
            id=self._next_request_id(),
            data=[],
        )

        self._eclient.reqHistoricalTicks(
            reqId=request.id,
            contract=contract,
            # startDateTime=start_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S"),
            startDateTime="",
            endDateTime=end_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S"),
            numberOfTicks=count,  # Max is 1000 per request.
            whatToShow="BID_ASK",
            useRth=1,
            ignoreSize=False,
            miscOptions=[],
        )

        return await self._wait_for_request(request)

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        self._log.debug(f"historicalTicksBidAsk {len(ticks)} ticks")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        for tick in ticks:
            tick.timestamp = self._parser.parse_datetime(tick.time)

        request.data.extend(ticks)
        if done:
            request.set_result(request.data)

    ################################################################################################
    # Request trade ticks

    async def request_trade_ticks(
        self,
        contract: IBContract,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        count: int = 1000,
    ) -> list[HistoricalTickLast]:
        assert start_time.tz is not None, "Timestamp is not timezone aware"
        assert end_time.tz is not None, "Timestamp is not timezone aware"
        assert start_time < end_time

        request = self._create_request(
            id=self._next_request_id(),
            data=[],
        )

        self._eclient.reqHistoricalTicks(
            reqId=request.id,
            contract=contract,
            startDateTime=start_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S"),
            endDateTime=end_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S"),
            numberOfTicks=count,
            whatToShow="TRADES",
            useRth=True,
            ignoreSize=False,
            miscOptions=[],
        )

        return await self._wait_for_request(request)

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        """
        Returns historical tick data when whatToShow=TRADES.
        """
        self._log.debug(f"historicalTicksLast {len(ticks)} ticks")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        for tick in ticks:
            tick.timestamp = self._parser.parse_datetime(tick.time)

        request.data.extend(ticks)

        if done:
            request.set_result(request.data)

    ################################################################################################
    # Subscribe ticks

    def subscribe_quote_ticks(
        self,
        contract: IBContract,
        callback: Callable,
    ) -> ClientSubscription:
        request_id = self._next_request_id()

        subscribe = functools.partial(
            self._eclient.reqTickByTickData,
            reqId=request_id,
            contract=contract,
            tickType="BidAsk",
            numberOfTicks=0,
            ignoreSize=True,
        )

        cancel = functools.partial(
            self._unsubscribe,
            request_id=request_id,
            cancel_func=functools.partial(self._eclient.cancelTickByTickData, reqId=request_id),
        )

        subscription = ClientSubscription(
            id=request_id,
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

        return subscription

    def tickByTickBidAsk(  # : Override the EWrapper
        self,
        reqId: int,
        time: int,
        bidPrice: float,
        askPrice: float,
        bidSize: Decimal,
        askSize: Decimal,
        tickAttribBidAsk: TickAttribBidAsk,  # tick-by-tick real-time bid/ask tick attribs (bit 0 – bid past low, bit 1 – ask past high).
    ):
        """
        Returns tick-by-tick data for tickType = "BidAsk".
        """
        self._log.debug(
            f"Received quote tick {reqId} {time}, {bidPrice}, {askPrice}, {bidSize}, {askSize}",
        )

        subscription = self._subscriptions.get(reqId)
        if subscription is None:
            self._log.debug(
                f"No subscription found for request_id {reqId}",
            )
            return  # no response found for request_id

        tick = HistoricalTickBidAsk()
        tick.time = time
        tick.priceBid = bidPrice
        tick.priceAsk = askPrice
        tick.sizeBid = bidSize
        tick.sizeAsk = askSize
        tick.timestamp = self._parser.parse_datetime(time)
        tick.tickAttribBidAsk = tickAttribBidAsk

        subscription.callback(tick)

    ################################################################################################
    # Realtime bars

    def subscribe_bars(
        self,
        contract: IBContract,
        what_to_show: WhatToShow,
        bar_size: BarSize,
        callback: Callable,
    ) -> ClientSubscription:
        if bar_size == BarSize._5_SECOND:
            return self._subscribe_realtime_bars(
                contract=contract,
                what_to_show=what_to_show,
                bar_size=bar_size,
                callback=callback,
            )
        else:
            return self._subscribe_historical_bars(
                contract=contract,
                what_to_show=what_to_show,
                bar_size=bar_size,
                callback=callback,
            )

    def _subscribe_realtime_bars(
        self,
        contract: IBContract,
        what_to_show: WhatToShow,
        bar_size: BarSize,
        callback: Callable,
    ) -> ClientSubscription:
        self._log.debug(
            f"Subscribing to realtime bars {contract.symbol} {contract.exchange} {bar_size!s} {what_to_show.name}",
        )

        request_id = self._next_request_id()

        cancel = functools.partial(
            self._unsubscribe, request_id=request_id, cancel_func=functools.partial(self._eclient.cancelRealTimeBars, reqId=request_id)
        )

        subscribe = functools.partial(
            self._eclient.reqRealTimeBars,
            reqId=request_id,
            contract=contract,
            barSize="",  # currently being ignored
            whatToShow=what_to_show.name,
            useRTH=True,
            realTimeBarsOptions=[],
        )

        subscription = ClientSubscription(
            id=request_id,
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

        return subscription

    def _subscribe_historical_bars(
        self,
        contract: IBContract,
        what_to_show: WhatToShow,
        bar_size: BarSize,
        callback: Callable,
    ) -> ClientSubscription:
        request_id = self._next_request_id()

        self._log.debug(
            f"Subscribing to historical bars {contract.symbol} {contract.exchange} {bar_size!s} {what_to_show.name}",
        )

        cancel = functools.partial(
            self._unsubscribe, request_id=request_id, cancel_func=functools.partial(self._eclient.cancelHistoricalData, reqId=request_id)
        )

        subscribe = functools.partial(
            self._eclient.reqHistoricalData,
            reqId=request_id,
            contract=contract,
            endDateTime="",
            durationStr=bar_size.to_duration().value,
            barSizeSetting=str(bar_size),
            whatToShow=what_to_show.value,
            useRTH=True,
            formatDate=1,
            keepUpToDate=True,
            chartOptions=[],
        )

        subscription = ClientSubscription(
            id=request_id,
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

        return subscription

    def _unsubscribe(
        self,
        request_id: int,
        cancel_func: Callable,
    ) -> None:
        subscription = self._subscriptions.get(request_id)
        if subscription is None:
            self._log.debug(f"No subscription found for request_id {request_id}")
            return

        cancel_func()
        del self._subscriptions[request_id]

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        """
        Returns updates in real time when keepUpToDate is set to True.
        """
        self._log.debug(f"Received historical bar reqId: {reqId}, {bar}")

        subscription = self._subscriptions.get(reqId)
        if subscription is None:
            self._log.debug(f"No subscription found for request_id {reqId}")
            return

        bar.timestamp = self._parser.parse_datetime(bar.date)
        subscription.callback(bar)

    def realtimeBar(
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Decimal,
        wap: Decimal,
        count: int,
    ):  # : Override the EWrapper
        """
        Returns updates in real time when subscribed to 5 second bars.
        """
        self._log.debug(
            f"Received realtime bar reqId: {reqId}, {time}, {open_} {high} {low}, {close} {volume} {wap} {count}",
        )
        subscription = self._subscriptions.get(reqId)
        if subscription is None:
            return  # no subscription found for request_id

        bar = BarData()
        bar.timestamp = self._parser.parse_datetime(time)
        bar.date = time
        bar.open = open_
        bar.high = high
        bar.low = low
        bar.close = close
        bar.volume = volume
        bar.wap = wap
        bar.barCount = count

        subscription.callback(bar)

    ################################################################################################
    # Accounts

    async def request_accounts(self) -> list[str]:
        request_id = self._request_id_map["accounts"]
        request = self._create_request(
            id=request_id,
        )

        self._eclient.reqManagedAccts()

        return await self._wait_for_request(request)

    def managedAccounts(self, accountsList: str):
        """
        Receives a comma-separated string with the managed account ids.
        """
        request_id = self._request_id_map["accounts"]
        request = self._requests.get(request_id)
        if request is None:
            return  # no response found for request_id

        request.set_result(accountsList.split(","))

    ################################################################################################
    # Subscribe account summary

    async def subscribe_account_updates(self, callback: Callable) -> ClientSubscription:
        """
        All account values and positions will be returned initially, and then there will
        only be updates when there is a change in a position, or to an account value
        every 3 minutes if it has changed.

        Only one account can be subscribed at a time

        """
        accounts = await self.request_accounts()

        request_id = self._next_request_id()

        subscribe = functools.partial(
            self._eclient.reqAccountUpdates,
            subscribe=True,
            acctCode=accounts[0],
        )

        cancel = functools.partial(
            self._eclient.reqAccountUpdates,
            subscribe=False,
            acctCode=accounts[0],
        )

        subscription = ClientSubscription(
            id=request_id,
            name="account_updates",
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

        return subscription

    async def request_portfolio(self) -> list[IBPortfolioEvent]:
        accounts = await self.request_accounts()

        request = self._create_request(
            id="portfolio",
            data=[],
        )

        self._eclient.reqAccountUpdates(
            subscribe=True,  # TODO: check for an active account update subscription first
            acctCode=accounts[0],
        )

        events = await self._wait_for_request(request)

        self._eclient.reqAccountUpdates(
            subscribe=False,
            acctCode=accounts[0],
        )

        for event in events:
            self._log.debug(repr(event))

        return events

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """
        This function is called only when ReqAccountUpdates on EEClientSocket object has
        been called.
        """
        self._log.debug("updateAccountValue")

    def updatePortfolio(
        self,
        contract: IBContract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ):
        """
        This function is called only when reqAccountUpdates on EEClientSocket object has
        been called.
        """
        self._log.debug(f"updatePortfolio {contract.conId}")

        request_id = self._request_id_map["portfolio"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug(f"No request found for id {request_id}")
            return

        request.data.append(
            IBPortfolioEvent(
                conId=contract.conId,
                position=position,
                marketPrice=marketPrice,
                marketValue=marketValue,
                averageCost=averageCost,
                unrealizedPNL=unrealizedPNL,
                realizedPNL=realizedPNL,
                accountName=accountName,
            ),
        )

    def updateAccountTime(self, timeStamp: str):
        self._log.debug("updateAccountTime")
        # self._account_update['time'] = timeStamp

    def accountDownloadEnd(self, accountName: str):
        """
        called after a batch updateAccountValue() and updatePortfolio() is sent.
        """
        self._log.debug("accountDownloadEnd")
        self._log.debug(repr(self._requests.get("portfolio")))

        request_id = self._request_id_map["portfolio"]
        request = self._requests.get(request_id)
        if request is None:
            return  # no response found for request_id

        request.set_result(request.data)

    ################################################################################################
    # Realtime historical schedule

    async def request_historical_schedule(self, contract: IBContract, durationStr: str | None = None) -> ListOfHistoricalSessions:
        request: ClientRequest = self._create_request(
            id=self._next_request_id(),
        )

        self._log.debug(f"reqHistoricalData: {request.id=}, {contract=}")

        self._eclient.reqHistoricalData(
            reqId=request.id,
            contract=contract,
            endDateTime="",
            durationStr=durationStr or "100 Y",
            # durationStr="5 D",
            barSizeSetting="1 day",
            whatToShow="SCHEDULE",
            useRTH=1,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )

        return await self._wait_for_request(request)

    def historicalSchedule(
        self,
        reqId: int,
        startDateTime: str,
        endDateTime: str,
        timeZone: str,
        sessions: ListOfHistoricalSessions,
    ):
        request = self._requests.get(reqId)
        if request is None:
            self._log.error(f"No request found for {reqId}")
            return

        data = [(session.refDate, session.startDateTime, session.endDateTime) for session in sessions]

        df = pd.DataFrame(data, columns=["day", "start", "end"])

        df.day = pd.to_datetime(df.day, format="%Y%m%d")
        df.start = pd.to_datetime(df.start, format="%Y%m%d-%H:%M:%S")
        df.end = pd.to_datetime(df.end, format="%Y%m%d-%H:%M:%S")
        df["timezone"] = timeZone

        df.sort_values(by=["day", "start"], inplace=True)

        request.set_result(df)

    # request.data.extend(
    #     [
    #         IBQuoteTick(
    #             name=request.name,
    #             time=parse_datetime(tick.time),
    #             bid_price=tick.priceBid,
    #             ask_price=tick.priceAsk,
    #             bid_size=tick.sizeBid,
    #             ask_size=tick.sizeAsk,
    #         )
    #         for tick in ticks
    #     ],
    # )

    # def marketDataType(self, reqId:TickerId, marketDataType:int):
    #     """TWS sends a marketDataType(type) callback to the API, where
    #     type is set to Frozen or RealTime, to announce that market data has been
    #     switched between frozen and real-time. This notification occurs only
    #     when market data switches between real-time and frozen. The
    #     marketDataType( ) callback accepts a reqId parameter and is sent per
    #     every subscription because different contracts can generally trade on a
    #     different schedule."""
    #     request = self._requests.get(reqId)
    #     if request is None:
    #         return  # no request found for request_id

    #     request.set_result(marketDataType)


# # handle event-driven based response without a request
# if reqId == -1:
#     self.execution_events.emit(event)
#     return

# request = self._requests.get(reqId)
# if request is None:
#     self._log.debug(f"no request for request_id: {reqId}")
#     return

# self._log.debug(f"request found with id {request.id}")


# handle request based response
