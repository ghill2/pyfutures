import asyncio
import functools

# fmt: on
import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import eventkit

# fmt: off
import pandas as pd
from ibapi import comm
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport as IBCommissionReport
from ibapi.common import BarData
from ibapi.common import ListOfHistoricalTickBidAsk
from ibapi.common import ListOfHistoricalTickLast
from ibapi.common import ListOfHistoricalSessions
from ibapi.common import OrderId
from ibapi.common import HistoricalTickBidAsk
from ibapi.common import TickAttribBidAsk
from ibapi.common import TickerId
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.decoder import Decoder
from ibapi.execution import Execution as IBExecution
from ibapi.execution import ExecutionFilter
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState as IBOrderState
from ibapi.wrapper import EWrapper


from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Component
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.uuid import UUID4
from pyfutures.adapters.interactive_brokers import IB_VENUE
from pyfutures.adapters.interactive_brokers.client.connection import Connection
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import ClientRequest
from pyfutures.adapters.interactive_brokers.client.objects import ClientSubscription
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBErrorEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBExecutionEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBOpenOrderEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBOrderStatusEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBPortfolioEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBPositionEvent
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime


class InteractiveBrokersClient(Component, EWrapper):
    
    _request_id_map = {
        # position request id is reserve for order
        # -1 reserve for no id from IB
        "executions": -2,
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
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        api_log_level: int = logging.ERROR,
        request_timeout_seconds: int | None = None,
    ):
        super().__init__(
            clock=clock,
            component_id=ClientId(f"{IB_VENUE.value}-{client_id:03d}"),
            component_name=f"{type(self).__name__}-{client_id:03d}",
            msgbus=msgbus,
            # config=NautilusConfig({"name": f"{type(self).__name__}-{client_id:03d}", "client_id": client_id},
            # config=NautilusConfig(name=f"{type(self).__name__}-{client_id:03d}", client_id=client_id),
        )

        # Events
        self.order_status_events = eventkit.Event("IBOrderStatusEvent")
        self.open_order_events = eventkit.Event("IBOpenOrderEvent")
        self.error_events = eventkit.Event("IBErrorEvent")
        self.execution_events = eventkit.Event("IBExecutionEvent")

        # Config
        self._loop = loop
        self._cache = cache

        self._requests = {}
        self._subscriptions = {}
        self._executions = {}  # hot cache

        names = logging.Logger.manager.loggerDict
        for name in names:
            if "ibapi" in name:
                logging.getLogger(name).setLevel(api_log_level)
                pass

        self._request_timeout_seconds = request_timeout_seconds

        self._conn = Connection(
            loop=loop,
            handler=self._handle_msg,
            host=host,
            port=port,
            client_id=client_id,
            subscriptions=self._subscriptions.values(),
        )

        self._client = EClient(wrapper=None)
        self._client.isConnected = lambda: True
        self._client.serverVersion = lambda: 176
        self._client.conn = self._conn

        self._request_id_seq = -10
        
        self._decoder = Decoder(wrapper=self, serverVersion=176)
        # self._log._is_bypassed = True

    @property
    def cache(self) -> Cache:
        return self._cache

    @property
    def subscriptions(self) -> list[ClientSubscription]:
        return self._subscriptions.values()

    @property
    def requests(self) -> list[ClientRequest]:
        return self._requests.values()
    
    @property
    def connection(self) -> Connection:
        return self._conn

    ################################################################################################
    # Connection

    async def reset(self) -> None:
        self._conn._reset()
    
    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected
    
    async def connect(self) -> None:
        await self._conn.connect()
    
    async def _handle_msg(self, msg: bytes) -> None:
        # self._log.debug(repr(msg))
        fields = comm.read_fields(msg)
        # self._log.debug(
        #     "Received fields: " + ",".join([x.decode(errors="backslashreplace") for x in fields]),
        # )
        self._decoder.interpret(fields)
        await asyncio.sleep(0)

    ################################################################################################
    # Responses

    def _next_req_id(self) -> int:
        current = self._request_id_seq
        self._request_id_seq -= 1
        return current

    def _create_request(
        self,
        data: list | dict | None = None,
        timeout_seconds: int | None = None,
        name: str | None = None,
        id: int | None = None,
    ) -> ClientRequest:
        if id is None:
            id: int = self._next_req_id()
        elif isinstance(id, str):
            id: int = self._request_id_map.get(id)
            if id is None:
                raise RuntimeError(f"No map for id {id}")
        else:
            raise RuntimeError

        request = ClientRequest(
            id=id,
            data=data,
            timeout_seconds=timeout_seconds or self._request_timeout_seconds,
            name=name,
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
        reqId: TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ) -> None:  # : Override the EWrapper
        # TODO: if reqId is negative, its part of a request

        event = IBErrorEvent(
            request_id=reqId,
            code=errorCode,
            message=errorString,
            advanced_order_reject_json=advancedOrderRejectJson,
        )

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
    # Order Execution

    def place_order(self, order: IBOrder) -> None:
        self._client.placeOrder(order.orderId, order.contract, order)

    def cancel_order(self, order_id: int, manual_cancel_order_time: str = "") -> None:
        self._client.cancelOrder(order_id, manual_cancel_order_time)

    ################################################################################################
    # Contract Details

    async def request_contract_details(self, contract: IBContract) -> list[IBContractDetails]:
        self._log.debug(
            f"Requesting contract details for:"
            f" tradingClass={contract.symbol},"
            f" symbol={contract.tradingClass},"
            f" exchange={contract.exchange},"
        )

        request = self._create_request(data=[])

        self._client.reqContractDetails(reqId=request.id, contract=contract)

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
        
        request.set_result(
            sorted(request.data, key=lambda x: x.contractMonth)
        )

    ################################################################################################
    # Request bars

    async def request_bars(
        self,
        contract: IBContract,
        bar_size: BarSize,
        what_to_show: WhatToShow,
        duration: Duration,
        end_time: pd.Timestamp = None,
        use_rth: bool = True,
        timeout_seconds: int | None = None,
    ) -> list[BarData]:
        request: ClientRequest = self._create_request(
            data=[],
            timeout_seconds=timeout_seconds,
        )

        endDateTime = end_time.strftime(format="%Y%m%d-%H:%M:%S") if end_time is not None else ""

        self._log.debug(f"reqHistoricalData: {request.id=}, {contract=}")

        self._client.reqHistoricalData(
            reqId=request.id,
            contract=contract,
            endDateTime=endDateTime,
            durationStr=duration,
            barSizeSetting=str(bar_size),
            whatToShow=what_to_show.name,
            useRTH=use_rth,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )

        # cancel=functools.partial(self._client.cancelHistoricalData, reqId=req_id),
        return await self._wait_for_request(request)

    def historicalData(self, reqId: int, bar: BarData):  # : Override the EWrapper
        request = self._requests.get(reqId)
        if request is None:
            return  # no request found for request_id

        request.data.append(
            IBBar(
                name=None,
                time=parse_datetime(bar.date),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                wap=bar.wap,
                count=bar.barCount,
            ),
        )

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
        request = self._create_request(id="next_order_id")

        self._client.reqIds(1)

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
        request = self._create_request(data=[], id="orders")

        self._client.reqOpenOrders()

        return await self._wait_for_request(request)

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
            self._log.debug("returning because the client is currently retrieving orders")
            return  # request processing, do not submit to execution handling

        event = IBOrderStatusEvent(
            order_id=orderId,
            status=status,
            # filled=filled,
            # remaining=remaining,
            # avg_fill_price=avgFillPrice,
            # perm_id=permId,
            # parent_id=parentId,
            # last_fill_price=lastFillPrice,
            # client_id=clientId,
            # why_held=whyHeld,
            # mkt_cap_price=mktCapPrice,
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
        to return estimated pre-trade marginand commission information in response
        to invoking IBApi.EClient.placeOrderwith a IBApi.Order object that has the flag
        IBApi.Order.WhatIf flag set to True

        OrderStatusReport

        """
        self._log.debug(f"openOrder {orderId}")

        if orderState.warningText != "":
            self._log.warning(f"order {orderId} has warning: {orderState.warningText}")

        event = IBOpenOrderEvent(
            conId=contract.conId,
            totalQuantity=order.totalQuantity,
            filledQuantity=order.filledQuantity,
            status=orderState.status,
            lmtPrice=order.lmtPrice,
            action=order.action,
            orderId=order.orderId,
            orderType=order.orderType,
            tif=order.tif,
            orderRef=order.orderRef,
        )

        request_id = self._request_id_map["orders"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug("no orders response, emitting event...")

            # TODO: make sure execution callbacks are not triggered on reconciliation
            self.open_order_events.emit(event)

            return

        request.data.append(event)
        self._log.debug(f"Received {len(request.data)} items")

    def openOrderEnd(self):
        """
        self._client.reqOpenOrders callback.
        """
        self._log.debug("openOrderEnd")

        request_id = self._request_id_map["orders"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug(f"No request found for id {request_id}")
            return

        request.set_result(request.data)

    ################################################################################################
    # Positions query

    async def request_positions(self) -> list[IBPositionEvent]:
        request = self._create_request(data=[], id="positions")

        self._client.reqPositions()

        return await self._wait_for_request(request)

    def position(  # : Override the EWrapper
        self,
        account: str,
        contract: IBContract,
        position: Decimal,
        avgCost: float,
    ) -> list[dict]:
        """
        This event returns real-time positions for all accounts in response to the
        reqPositions() method.
        """
        self._log.debug("position")

        request_id = self._request_id_map["positions"]
        request = self._requests.get(request_id)
        if request is None:
            self._log.debug("no request for positions... returning")
            return  # no response found for request_id

        request.data.append(
            IBPositionEvent(
                conId=contract.conId,
                quantity=position,
            ),
        )

    def positionEnd(self):
        """
        This is called once all position data for a given request are received and
        functions as an end marker for the position() data.
        """
        self._log.debug("positionEnd")

        request_id = self._request_id_map["positions"]
        request = self._requests.get(request_id)
        if request is None:
            return  # no response found for request_id

        request.set_result(request.data)

    ################################################################################################
    # Executions query

    async def request_executions(self):
        request = self._create_request(data=[])

        filter = ExecutionFilter()
        filter.client_id = self._client_id

        self._client.reqExecutions(
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
        """
        self._log.debug(f"execDetails reqId={reqId}")

        event = IBExecutionEvent(
            reqId=reqId,
            conId=contract.conId,
            orderId=execution.orderId,
            execId=execution.execId,
            side=execution.side,
            shares=execution.shares,
            price=execution.price,
            commission=None,  # filled on commissionReport
            commissionCurrency=None,  # filled on commissionReport
            time=parse_datetime(execution.time),
        )

        # handle event-driven based response
        if reqId == -1:
            self._executions[execution.execId] = event
            return

        request = self._requests.get(reqId)
        if request is None:
            self._log.debug(f"no request for request_id: {reqId}")
            return

        self._log.debug(f"request found with id {request.id}")

        # handle request based response
        request.data.append(event)

    def commissionReport(self, commissionReport: IBCommissionReport):  # : Override the EWrapper
        """
        The commissionReport() callback is triggered as follows:

        - immediately after a trade execution
        - by calling reqExecutions().

        """
        self._log.debug("commissionReport")

        # self._commission_reports[commissionReport.execId] = commissionReport
        event = self._executions[commissionReport.execId]
        event.commission = commissionReport.commission
        event.commissionCurrency = commissionReport.currency

        # TODO: make sure reconciliation does not emit order filled events
        self.execution_events.emit(event)

    def execDetailsEnd(self, reqId: int):  # : Override the EWrapper
        """
        This function is called once all executions have been sent to a client in
        response to reqExecutions().
        """
        request = self._requests.get(reqId)
        if request is None:
            self._log.debug(f"no request for request_id: {reqId}")
            return  # no response found for request_id

        # add commission info
        # for event in request.data:
        #     report = self._commission_reports[event.execId]
        #     event.commission = report.commission
        #     event.commissionCurrency = report.currency

        request.set_result(request.data)

    ################################################################################################
    # Account and Portfolio
    async def request_account_summary(self) -> dict:
        """
        Call this method to request and keep up to date the data that appears on the TWS
        Account Window Summary tab.

        The data is returned by accountSummary().

        """
        request = self._create_request(data={})

        self._client.reqAccountSummary(
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
        request = self._create_request()

        try:
            self._client.reqHeadTimeStamp(
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
        # print("headTimestamp")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id

        request.set_result(pd.to_datetime(headTimestamp, format="%Y%m%d-%H:%M:%S", utc=True))

    ################################################################################################
    # Request quote ticks

    async def request_last_quote_tick(
        self,
        contract: IBContract,
        use_rth: bool = True,
    ) -> IBQuoteTick:
        self._log.debug(f"Requesting last quote tick for {contract.symbol}")
        quotes = await self.request_quote_ticks(
            contract=contract,
            count=1,
            end_time=pd.Timestamp.utcnow(),
            use_rth=use_rth,
        )
        return None if len(quotes) == 0 else quotes[-1]

    async def request_first_quote_tick(
        self,
        contract: IBContract,
        use_rth: bool = True,
    ) -> IBQuoteTick | None:
        self._log.debug(f"Requesting last quote tick for {contract.symbol}")
        head_timestamp = await self.request_head_timestamp(
            contract=contract,
            what_to_show=WhatToShow.BID_ASK,
            use_rth=use_rth,
        )
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
        count: int = 1000,
        start_time: pd.Timestamp | None = None,
        end_time: pd.Timestamp | None = None,
        use_rth: bool = True,
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
        
        # TODO assert start_time is tz-aware
        # TODO assert end_time is tz-aware
        # assert start_time is not None and end_time is not None
        
        request = self._create_request(data=[])
        
        if start_time is None:
            start_time = ""
        else:
            start_time = start_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S")
            
        if end_time is None:
            end_time = pd.Timestamp.utcnow()
        end_time = end_time.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S")
        
        self._client.reqHistoricalTicks(
            reqId=request.id,
            contract=contract,
            startDateTime=start_time,
            endDateTime=end_time,
            numberOfTicks=count,  # Max is 1000 per request.
            whatToShow="BID_ASK",
            useRth=use_rth,
            ignoreSize=False,
            miscOptions=[],
            # formatDate=1,
        )

        return await self._wait_for_request(request)

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        self._log.debug(f"historicalTicksBidAsk {len(ticks)} ticks")

        request = self._requests.get(reqId)
        if request is None:
            return  # no response found for request_id
        
        request.data.extend(ticks)
        if done:
            request.set_result(request.data)

    ################################################################################################
    # Request trade ticks

    async def request_trade_ticks(
        self,
        name: str,
        contract: IBContract,
        count: int,
        end_time: pd.Timestamp = None,
        use_rth: bool = True,
    ) -> list[IBTradeTick]:
        request = self._create_request(data=[], name=name)

        if end_time is None:
            end_time = pd.Timestamp.utcnow()

        self._client.reqHistoricalTicks(
            reqId=request.id,
            contract=contract,
            startDateTime="",
            endDateTime=end_time.strftime("%Y%m%d %H:%M:%S %Z"),
            numberOfTicks=count,
            whatToShow="TRADES",
            useRth=use_rth,
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

        request.data.extend(
            [
                IBTradeTick(
                    name=request.name,
                    time=parse_datetime(tick.time),
                    price=tick.price,
                    size=tick.size,
                    exchange=tick.exchange,
                    conditions=tick.specialConditions,
                )
                for tick in ticks
            ],
        )

        if done:
            request.set_result(request.data)

    ################################################################################################
    # Realtime bars

    def subscribe_bars(
        self,
        name: str,
        contract: IBContract,
        what_to_show: WhatToShow,
        bar_size: BarSize,
        callback: Callable,
        use_rth: bool = True,
    ) -> ClientSubscription:
        request_id = self._next_req_id()

        if bar_size == BarSize._5_SECOND:
            
            self._log.debug(
                f"Requesting realtime bars {contract.symbol} {contract.exchange} {bar_size!s} {what_to_show.name}",
            )

            cancel = functools.partial(self._client.cancelRealTimeBars, reqId=request_id)

            subscribe = functools.partial(
                self._client.reqRealTimeBars,
                reqId=request_id,
                contract=contract,
                barSize="",  # currently being ignored
                whatToShow=what_to_show.name,
                useRTH=use_rth,
                realTimeBarsOptions=[],
            )

        else:
            self._log.debug(
                f"Requesting realtime bars {contract.symbol} {contract.exchange} {bar_size!s} {what_to_show.name}",
            )

            cancel = functools.partial(self._client.cancelHistoricalData, reqId=request_id)

            subscribe = functools.partial(
                self._client.reqHistoricalData,
                reqId=request_id,
                contract=contract,
                endDateTime="",
                durationStr=bar_size.to_duration().value,
                barSizeSetting=str(bar_size),
                whatToShow=what_to_show.value,
                useRTH=use_rth,
                formatDate=1,
                keepUpToDate=True,
                chartOptions=[],
            )

        subscription = ClientSubscription(
            id=request_id,
            name=name,
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

    async def unsubscribe(
        self,
        name: str,
    ) -> None:
        subscription = self._subscriptions.get(name)
        if subscription is None:
            return  # no subscription for name

        subscription.cancel()
        del self._subscriptions[name]

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        """
        Returns updates in real time when keepUpToDate is set to True.
        """
        self._log.debug(
            f"Received realtime bar {reqId}, {bar.date}, {bar.open_} {bar.high} {bar.low}, {bar.close} {bar.volume} {bar.wap} {bar.count}",
        )

        subscription = self._requests.get(reqId)
        if subscription is None:
            return  # no subscription found for request_id

        subscription.callback(
            IBBar(
                reqId=reqId,
                time=parse_datetime(bar.time),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                wap=bar.wap,
                count=bar.barCount,
            ),
        )

    def realtimeBar(
        self,
        reqId: str,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Decimal,
        wap: Decimal,
        count: int,
    ):  # : Override the EWrapper
        self._log.debug(
            f"Received realtime bar {reqId}, {time}, {open_} {high} {low}, {close} {volume} {wap} {count}",
        )

        subscription = self._requests.get(reqId)
        if subscription is None:
            return  # no subscription found for request_id

        subscription.callback(
            IBBar(
                name=subscription.name,
                time=parse_datetime(time),  # unix_nanos_to_dt(secs_to_nanos(int()))
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                wap=wap,
                count=count,
            ),
        )

    ################################################################################################
    # Realtime ticks

    def subscribe_quote_ticks(
        self,
        name: str,
        contract: IBContract,
        callback: Callable,
    ):
        request_id = self._next_req_id()

        subscribe = functools.partial(
            self._client.reqTickByTickData,
            reqId=request_id,
            contract=contract,
            tickType="BidAsk",
            numberOfTicks=0,
            ignoreSize=True,
        )

        cancel = functools.partial(self._client.cancelTickByTickData, reqId=request_id)

        subscription = ClientSubscription(
            id=request_id,
            name=name,
            subscribe=subscribe,
            cancel=cancel,
            callback=callback,
        )

        self._subscriptions[request_id] = subscription

        subscription.subscribe()

    def tickByTickBidAsk(  # : Override the EWrapper
        self,
        reqId: int,
        time: int,
        bidPrice: float,
        askPrice: float,
        bidSize: Decimal,
        askSize: Decimal,
        tickAttribBidAsk: TickAttribBidAsk,
    ):
        """
        Returns tick-by-tick data for tickType = "BidAsk".
        """
        self._log.debug(
            f"Received quote tick {reqId} {time}, {bidPrice}, {askPrice}, {bidSize}, {askSize}",
        )

        subscription = self._subscriptions.get(reqId)
        if subscription is None:
            return  # no response found for request_id

        subscription.callback(
            IBQuoteTick(
                name=subscription.name,
                time=parse_datetime(time),
                bid_price=bidPrice,
                ask_price=askPrice,
                bid_size=bidSize,
                ask_size=askSize,
            ),
        )

    async def request_accounts(self) -> list[str]:
        request = self._create_request(id="accounts")

        self._client.reqManagedAccts()

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

        request_id = self._next_req_id()

        subscribe = functools.partial(
            self._client.reqAccountUpdates,
            subscribe=True,
            acctCode=accounts[0],
        )

        cancel = functools.partial(
            self._client.reqAccountUpdates,
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

        request = self._create_request(data=[], id="portfolio")

        self._client.reqAccountUpdates(
            subscribe=True,  # TODO: check for an active account update subscription first
            acctCode=accounts[0],
        )

        events = await self._wait_for_request(request)

        self._client.reqAccountUpdates(
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
            return  # no response found for request_id

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
        This is called after a batch updateAccountValue() and updatePortfolio() is sent.
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
    
    
    async def request_historical_schedule(
        self,
        contract: IBContract,
        durationStr: str | None = None
    ) -> ListOfHistoricalSessions:
        
        request: ClientRequest = self._create_request()

        self._log.debug(f"reqHistoricalData: {request.id=}, {contract=}")

        self._client.reqHistoricalData(
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

        data = [
            (session.refDate, session.startDateTime, session.endDateTime)
            for session in sessions
        ]

        df = pd.DataFrame(data, columns=["day", "start", "end"])
        
        df.day = pd.to_datetime(df.day, format="%Y%m%d")
        df.start = pd.to_datetime(df.start, format="%Y%m%d-%H:%M:%S")
        df.end = pd.to_datetime(df.end, format="%Y%m%d-%H:%M:%S")
        df['timezone'] = timeZone
        
        df.sort_values(by=['day', 'start'], inplace=True)
        
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