import pytest


# We are not using these tests, do them last:
# request_open_orders
# request_executions
# request_quote_ticks
# request_first_quote_tick
# request_last_quote_tick
# subscribe_realtime_bars ()
# request_trade_ticks
# subscribe_quote_ticks
# request_account_summary
# subscribe_account_updates
# request_portfolio
# request_historical_schedule
# request_last_bar
# request_market_data


@pytest.mark.asyncio()
async def test_request_last_quote_tick(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.exchange = "CME"
    contract.secType = "CONTFUT"

    await client.connect()
    last = await asyncio.wait_for(
        client.request_last_quote_tick(
            contract=contract,
        ),
        2,
    )
    assert isinstance(last, HistoricalTickBidAsk)


@pytest.mark.skip(reason="trade ticks return 0 for this contract")
@pytest.mark.asyncio()
async def test_request_trade_ticks(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    contract = IBContract()
    contract.conId = 553444806
    contract.exchange = "ICEEUSOFT"

    trades = await asyncio.wait_for(
        client.request_trade_ticks(
            name="test",
            contract=contract,
            count=50,
        ),
        2,
    )

    assert len(trades) == 51
    assert all(isinstance(trade, IBTradeTick) for trade in trades)


@pytest.mark.skip(reason="unused")
@pytest.mark.asyncio()
async def test_subscribe_account_updates(event_loop):
    client = ClientStubs.client(loop=event_loop)
    callback = Mock()

    await client.subscribe_account_updates(callback=callback)

    await asyncio.sleep(5)

    assert callback.call_count > 0


@pytest.mark.asyncio()
async def test_request_timezones(event_loop):
    pass


@pytest.mark.skip(reason="unused")
@pytest.mark.asyncio()
async def test_import_schedules(event_loop):
    pass


@pytest.mark.asyncio()
async def test_request_executions(client):
    await client.connect()
    executions = await client.request_executions()
    print(executions)


@pytest.mark.asyncio()
async def test_request_portfolio(client):
    await client.connect()
    await client.request_portfolio()


@pytest.mark.asyncio()
async def test_request_historical_schedule(client, contract):
    await client.connect()
    df = await client.request_historical_schedule(contract=contract)
    print(df.iloc[:49])


@pytest.mark.asyncio()
async def test_request_open_orders(client):
    await client.connect()
    orders = await client.request_open_orders()
    print(orders)


@pytest.mark.asyncio()
async def test_request_open_orders(self):
    # Arrange
    def send_mocked_response(*args, **kwargs):
        self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
        self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
        self.client.openOrderEnd()

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqOpenOrders = send_mock

    # Act
    orders = await self.client.request_open_orders()

    # Assert
    assert len(orders) == 2
    assert all(isinstance(o, IBOpenOrderEvent) for o in orders)
    send_mock.assert_called_once()


@pytest.mark.asyncio()
async def test_request_open_orders_no_event_emit(self):
    """
    when there is an active open orders request an event should not be emitted
    """

    # Arrange
    def send_mocked_response(*args, **kwargs):
        self.client.openOrder(4, IBContract(), IBOrder(), IBOrderState())
        self.client.orderStatus(
            orderId=4,
            status="FILLED",
            filled=Decimal("1"),
            remaining=Decimal("1"),
            avgFillPrice=1.23,
            permId=5,
            parentId=6,
            lastFillPrice=1.43,
            clientId=1,
            whyHeld="reason",
            mktCapPrice=1.76,
        )
        self.client.openOrderEnd()

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqOpenOrders = send_mock

    open_order_callback_mock = AsyncMock()
    self.client.open_order_events += open_order_callback_mock

    order_status_callback_mock = AsyncMock()
    self.client.order_status_events += order_status_callback_mock

    # Act
    await self.client.request_open_orders()

    # Assert
    order_status_callback_mock.assert_not_called()
    open_order_callback_mock.assert_not_called()


@pytest.mark.asyncio()
async def test_request_executions_returns_expected(self):
    # Arrange
    def send_mocked_response(*args, **kwargs):
        execution = IBExecution()
        execution.execId = 1
        execution.time = "20231116-12:07:51"

        report = IBCommissionReport()
        report.execId = execution.execId
        self.client.execDetails(-10, IBContract(), execution)
        self.client.commissionReport(report)

        execution = IBExecution()
        execution.execId = 2
        execution.time = "20231116-12:07:51"

        report = IBCommissionReport()
        report.execId = execution.execId
        self.client.execDetails(-10, IBContract(), execution)
        self.client.commissionReport(report)

        self.client.execDetailsEnd(-10)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqExecutions = send_mock

    # Act
    executions = await self.client.request_executions(client_id=1)

    # Assert
    assert len(executions) == 2
    assert all(isinstance(e, IBExecutionEvent) for e in executions)
    send_mock.assert_called_once()


@pytest.mark.asyncio()
async def test_request_executions_no_event_emit(self):
    # Arrange
    def send_mocked_response(*args, **kwargs):
        execution = IBExecution()
        execution.execId = 1
        execution.time = "20231116-12:07:51"

        report = IBCommissionReport()
        report.execId = execution.execId
        self.client.execDetails(-10, IBContract(), execution)
        self.client.commissionReport(report)

        execution = IBExecution()
        execution.execId = 2
        execution.time = "20231116-12:07:51"

        report = IBCommissionReport()
        report.execId = execution.execId
        self.client.execDetails(-10, IBContract(), execution)
        self.client.commissionReport(report)

        self.client.execDetailsEnd(-10)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqExecutions = send_mock

    callback_mock = AsyncMock()
    self.client.execution_events += callback_mock

    # Act
    await self.client.request_executions(client_id=1)

    # Assert
    callback_mock.assert_not_called()


@pytest.mark.asyncio()
async def test_request_quote_ticks(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    contract = IBContract()
    contract.conId = 553444806
    contract.exchange = "ICEEUSOFT"

    quotes = await asyncio.wait_for(
        client.request_quote_ticks(
            name="test",
            contract=contract,
            count=50,
        ),
        2,
    )

    assert len(quotes) == 54
    assert all(isinstance(quote, HistoricalTickBidAsk) for quote in quotes)


@pytest.mark.asyncio()
async def test_request_quote_ticks(self):
    # Act
    #
    contract = Contract()

    tick = HistoricalTickBidAsk()
    tick.time = 1700069390
    start_time = pd.Timestamp("2023-01-01 08:00:00", tz="UTC")
    end_time = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")

    def send_mocked_response(*args, **kwargs):
        self.client.historicalTicksBidAsk(-10, [tick], False)
        self.client.historicalTicksBidAsk(-10, [tick], True)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqHistoricalTicks = send_mock

    # Act
    quotes = await self.client.request_quote_ticks(
        contract=contract,
        start_time=start_time,
        end_time=end_time,
        count=2,
    )

    # Assert
    assert len(quotes) == 2
    assert all(isinstance(q, HistoricalTickBidAsk) for q in quotes)
    assert quotes[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
    assert quotes[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
    assert send_mock.call_args_list[0][1] == dict(
        reqId=-10,
        contract=contract,
        startDateTime="20230101-08:00:00",
        endDateTime="20230101-12:00:00",
        numberOfTicks=2,
        whatToShow="BID_ASK",
        useRth=True,
        ignoreSize=False,
        miscOptions=[],
    )


@pytest.mark.asyncio()
async def test_request_trade_ticks(self):
    # Arrange
    contract = Contract()

    tick = HistoricalTickLast()
    tick.time = 1700069390
    start_time = pd.Timestamp("2023-01-01 08:00:00", tz="UTC")
    end_time = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")

    def send_mocked_response(*args, **kwargs):
        self.client.historicalTicksLast(-10, [tick], False)
        self.client.historicalTicksLast(-10, [tick], True)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqHistoricalTicks = send_mock

    # Act
    trades = await self.client.request_trade_ticks(
        contract=contract,
        start_time=start_time,
        end_time=end_time,
        count=2,
    )

    # Assert
    assert len(trades) == 2
    assert all(isinstance(q, HistoricalTickLast) for q in trades)
    assert trades[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
    assert trades[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
    assert send_mock.call_args_list[0][1] == dict(
        reqId=-10,
        contract=contract,
        startDateTime="20230101-08:00:00",
        endDateTime="20230101-12:00:00",
        numberOfTicks=2,
        whatToShow="TRADES",
        useRth=True,
        ignoreSize=False,
        miscOptions=[],
    )


@pytest.mark.asyncio()
async def test_subscribe_quote_ticks(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    callback_mock = Mock()

    contract = IBContract()
    # contract.conId = 553444806
    # contract.exchange = "ICEEUSOFT"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"
    contract.symbol = "EUR"
    contract.currency = "GBP"

    client.subscribe_quote_ticks(
        contract=contract,
        callback=callback_mock,
    )

    async def wait_for_quote_tick():
        while callback_mock.call_count == 0:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_quote_tick(), 10)

    assert callback_mock.call_count > 0


@pytest.mark.asyncio()
async def test_subscribe_quote_ticks_sends_expected(self):
    # Arrange
    contract = Contract()
    send_mock = Mock()
    self.client._eclient.reqTickByTickData = send_mock

    # Act
    subscription = self.client.subscribe_quote_ticks(
        contract=contract,
        callback=Mock(),
    )
    assert isinstance(subscription, ClientSubscription)

    # Assert
    sent_kwargs = send_mock.call_args_list[0][1]
    assert sent_kwargs == dict(
        reqId=-10,
        contract=contract,
        tickType="BidAsk",
        numberOfTicks=0,
        ignoreSize=True,
    )


@pytest.mark.asyncio()
async def test_subscribe_quote_ticks_returns_expected(self):
    tickAttribBidAsk = TickAttribBidAsk()

    def send_mocked_response(*args, **kwargs):
        self.client.tickByTickBidAsk(
            reqId=-10,
            time=1700069390,
            bidPrice=1.1,
            askPrice=1.2,
            bidSize=Decimal("1"),
            askSize=Decimal("1"),
            tickAttribBidAsk=tickAttribBidAsk,
        )

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqTickByTickData = send_mock

    callback_mock = Mock()

    # Act
    self.client.subscribe_quote_ticks(
        contract=Contract(),
        callback=callback_mock,
    )

    # Assert
    expected = dict(
        timestamp=pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC"),
        time=1700069390,
        priceBid=1.1,
        priceAsk=1.2,
        sizeBid=Decimal("1"),
        sizeAsk=Decimal("1"),
        tickAttribBidAsk=tickAttribBidAsk,
    )
    tick_response = callback_mock.call_args_list[0][0][0]
    assert tick_response.__dict__ == expected


@pytest.mark.asyncio()
async def test_unsubscribe_quote_ticks(self):
    # Arrange
    self.client._eclient.reqTickByTickData = Mock()

    cancel_mock = Mock()
    self.client._eclient.cancelTickByTickData = cancel_mock

    # Act
    subscription = self.client.subscribe_quote_ticks(
        contract=Contract(),
        callback=Mock(),
    )
    subscription.cancel()

    # Assert
    cancel_mock.assert_called_once_with(reqId=-10)
    assert subscription not in self.client.subscriptions


# bytestring version test that we decided to not test for yet
@pytest.mark.skip(reason="can fail due to too many orders")
@pytest.mark.asyncio()
async def test_place_limit_order(client, contract):
    await client.connect()
    detail = await client.request_front_contract_details(contract)

    order = IBOrder()
    order.contract = detail.contract

    # LIMIT order
    order.orderId = await client.request_next_order_id()
    order.orderRef = str(UUID4())  # client_order_id
    order.orderType = "LMT"  # order_type
    order.totalQuantity = detail.minSize
    order.action = "BUY"  # side
    # order.lmtPrice = 2400.0  # price
    # order.tif = "GTC"  # time in force

    client.place_order(order)


@pytest.mark.asyncio()
async def test_limit_order_accepted(event_loop):
    """
    INFO:InteractiveBrokersClient:openOrder 14, orderStatus Submitted, commission: 1.7976931348623157e+308, completedStatus:
    """
    client = ClientStubs.client(loop=event_loop)
    await client.connect()

    order = IBOrder()

    order.orderId = await client.request_next_order_id()
    order.orderRef = str(UUID4())
    order.orderType = "LMT"
    order.totalQuantity = Decimal("1")
    order.action = "BUY"
    order.tif = "GTC"

    contract = IBContract()
    contract.tradingClass = "DC"
    contract.symbol = "DA"
    contract.secType = "FUT"
    contract.exchange = "CME"

    details = await client.request_front_contract_details(contract)
    contract = details.contract

    order.contract = contract

    quote = await client.request_last_quote_tick(contract)

    min_tick = details.minTick * details.priceMagnifier
    order.lmtPrice = quote.priceAsk - (min_tick * 1000)

    client.place_order(order)

    while True:
        await asyncio.sleep(0)


# bytestring version tests
@pytest.mark.skip(reason="TODO")
@pytest.mark.asyncio()
async def test_request_quote_ticks(client, dc_cont_contract):
    await client.connect()
    await client.request_market_data_type(4)
    contract = await client.request_front_contract(dc_cont_contract)
    ticks = await client.request_quote_ticks(
        contract=contract,
        end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
        count=5,
    )
    tick = ticks[0]
    assert tick.time == 1711124972
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:32+00:00")
    assert tick.priceBid == 15.6
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 2
    assert tick.sizeAsk == 1

    tick = ticks[1]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 8
    assert tick.sizeAsk == 3

    tick = ticks[2]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 7
    assert tick.sizeAsk == 3

    tick = ticks[3]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 3
    assert tick.sizeAsk == 3

    tick = ticks[4]
    assert tick.time == 1711124995
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:55+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 2
    assert tick.sizeAsk == 3

    tick = ticks[5]
    assert tick.time == 1711124999
    assert tick.timestamp == pd.Timestamp("2024-03-22 16:29:59+00:00")
    assert tick.priceBid == 15.59
    assert tick.priceAsk == 15.62
    assert tick.sizeBid == 4
    assert tick.sizeAsk == 3

    print(len(ticks))
    # assert len(ticks) == 51
    # assert all(isinstance(trade, IBTradeTick) for trade in trades)


@pytest.mark.skip(reason="universe WIP")
@pytest.mark.asyncio()
async def test_request_first_quote_tick_universe(client):
    await client.connect()

    rows = IBTestProviderStubs.universe_rows()
    for row in rows:
        if row.contract_cont.exchange not in ["CME", "CBOT", "NYMEX"]:
            continue

        await client.request_market_data_type(4)
        contract = await client.request_front_contract(row.contract_cont)

        # first_tick = await client.request_first_quote_tick(contract=contract)
        ticks = await client.request_first_quote_tick(
            contract=contract,
            # end_time=pd.Timestamp("2024-03-22 16:30:30+00:00"),
            # start_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
            # count=1,
        )

        print(len(ticks))
        if len(ticks) > 1:
            assert False

        for tick in ticks:
            print(tick.time)
            print(tick.timestamp)
            print(tick.priceBid)
            print(tick.priceAsk)
            print(tick.sizeBid)
            print(tick.sizeAsk)


@pytest.mark.skip(reason="Cant find start time value?")
@pytest.mark.asyncio()
async def test_request_first_quote_tick(client, dc_contract):
    await client.connect()

    await client.request_market_data_type(4)
    contract = await client.request_front_contract(dc_contract)

    # first_tick = await client.request_first_quote_tick(contract=contract)
    # ticks = await client.request_first_quote_tick(
    #     contract=contract,
    #     # end_time=pd.Timestamp("2024-03-22 16:30:30+00:00"),
    #     # start_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
    #     # count=1,
    # )
    print("WHAT_TO_SHOW")
    # 2022-03-31 22:00:00+00:00 <-- req_head
    # ticks = await client.request_quote_ticks(
    #     contract=contract,
    #     count=1000,
    #     end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
    #     whatToShow="BID_ASK",
    # )
    # ts = []
    # for detail in details:
    #     head_timestamp = await client.request_head_timestamp(
    #         contract=detail.contract, what_to_show=WhatToShow.BID_ASK
    #     )
    #     # print("HEAD TIMESTAMP", head_timestamp)
    #     ts.append((detail.contract.lastTradeDateOrContractMonth, head_timestamp))
    #     await asyncio.sleep(0.5)
    # print(ts)
    ticks = await client.request_quote_ticks(
        contract=contract,
        count=1000,
        start_time=pd.Timestamp("2022-03-31 22:00:00+00:00"),
        end_time=pd.Timestamp.utcnow(),
    )

    #
    # print(len(ticks))
    # if len(ticks) > 1:
    # assert False
    #
    for tick in ticks:
        print(tick.time)
        print(tick.timestamp)
        print(tick.priceBid)
        print(tick.priceAsk)
        print(tick.sizeBid)
        print(tick.sizeAsk)


# last_tick = await client.request_last_quote_tick(
#     contract=contract,
# )

# trade_ticks = await client.request_trade_ticks(
#     contract=contract,
#     start_time=end_time - pd.Timedelta(hours=1),
#     end_time=pd.Timestamp("2024-03-22 16:30:00+00:00"),
#     count=50,
# )


# assert quote ticks
# assert len(quotes) == 54
# assert all(isinstance(quote, HistoricalTickBidAsk) for quote in quotes)
#
# last quote_tick
# assert isinstance(last, HistoricalTickBidAsk)
#
# trade ticks
# assert len(trades) == 51
# assert all(isinstance(trade, IBTradeTick) for trade in trades)
#
# bars
# assert all(isinstance(bar, BarData) for bar in bars)
# assert len(bars) > 0
#
# @pytest.mark.skip(reason="flakey if market not open")
@pytest.mark.asyncio()
async def test_subscribe_realtime_bars(event_loop):
    client = ClientStubs.client(loop=event_loop)
    client.request_market_data_type(4)
    callback_mock = Mock()

    contract = IBContract()
    # contract.conId = 553444806
    # contract.exchange = "ICEEUSOFT"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"
    contract.symbol = "EUR"
    contract.currency = "GBP"

    client._subscribe_realtime_bars(
        contract=contract,
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._5_SECOND,
        callback=callback_mock,
    )

    async def wait_for_bar():
        while callback_mock.call_count == 0:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_bar(), 20)

    assert callback_mock.call_count > 0


pytest.mark.asyncio()


async def test_request_contract_details_returns_expected(self):
    # Arrange
    contract = Contract()

    def send_mocked_response(*args, **kwargs):
        self.client.contractDetails(-10, IBContractDetails())
        self.client.contractDetailsEnd(-10)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqContractDetails = send_mock

    # Act
    results = await self.client.request_contract_details(contract)

    # Assert
    assert isinstance(results, list)
    assert all(isinstance(x, IBContractDetails) for x in results)
    assert send_mock.call_args_list[0][1] == {
        "reqId": -10,
        "contract": contract,
    }


@pytest.mark.skip(reason="hangs when running entire test suite")
@pytest.mark.asyncio()
async def test_queue_processes_messages(self):
    # Arrange
    mock_connection = Mock()
    mock_connection.connect = AsyncMock()
    mock_connection.is_connected = lambda _: True
    mock_connection.sendMsg = Mock()
    self.client._connection = mock_connection
    await self.client.connect()

    # Act
    self.client.sendMsg(b"message1")
    self.client.sendMsg(b"message2")
    await asyncio.sleep(0)

    # Assert
    calls = mock_connection.sendMsg.call_args_list
    assert calls[0][0][0] == b"message1"
    assert calls[1][0][0] == b"message2"


@pytest.mark.asyncio()
async def test_request_next_order_id(self):
    """
    ibapi for next order_id
    convert to bytestring tests
    """

    # Arrange
    def send_mocked_response(*args, **kwargs):
        self.client.nextValidId(4)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqIds = send_mock

    # Act
    next_id = await self.client.request_next_order_id()

    assert next_id == 4
    send_mock.assert_called_once_with(1)


@pytest.mark.skip(reason="broken after re-connect changes")
@pytest.mark.asyncio()
async def test_request_bars_daily(self):
    # Arrange
    contract = Contract()

    end_time = pd.Timestamp("2023-11-16", tz="UTC")

    def send_mocked_response(*args, **kwargs):
        bar1 = BarData()
        bar1.date = 1700069390  # TODO: find correct timestamp format
        bar2 = BarData()
        bar2.date = 1700069390  # TODO: find correct timestamp format
        self.client.historicalData(-10, bar2)
        self.client.historicalData(-10, bar1)
        self.client.historicalDataEnd(-10, "", "")

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqHistoricalData = send_mock

    # Act
    bars = await client.request_bars(
        contract=contract,
        bar_size=BarSize._1_DAY,
        duration=Duration(4, Frequency.DAY),
        what_to_show=WhatToShow.BID,
        end_time=end_time,
    )

    # Assert
    assert len(bars) == 2
    assert isinstance(bars, list)
    assert all(isinstance(bar, BarData) for bar in bars)
    assert bars[0].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")
    assert bars[1].timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")

    sent_kwargs = send_mock.call_args_list[0][1]
    assert sent_kwargs == dict(
        reqId=-10,
        contract=contract,
        endDateTime="20231116-00:00:00",
        durationStr="4 D",
        barSizeSetting="1 day",
        whatToShow="BID",
        useRTH=True,
        formatDate=2,
        keepUpToDate=False,
        chartOptions=[],
    )


@pytest.mark.asyncio()
async def test_request_accounts(self):
    # Arrange
    def send_mocked_response(*args, **kwargs):
        self.client.managedAccounts("DU1234567,DU1234568")

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqManagedAccts = send_mock

    # Act
    accounts = await self.client.request_accounts()

    # Assert
    assert accounts == ["DU1234567", "DU1234568"]
    send_mock.assert_called_once()


# REDUNDANT: request_first_quote_tick executes this
@pytest.mark.asyncio()
async def test_request_head_timestamp_single(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()
    contract = IBContract()
    contract.conId = 553444806
    contract.exchange = "ICEEUSOFT"

    timestamp = await client.request_head_timestamp(
        contract=contract,
        what_to_show=WhatToShow.BID,
    )
    assert str(timestamp) == "2022-03-29 08:00:00+00:00"


# MERGE THESE 2 TESTS BELOW
@pytest.mark.skip(reason="flakey if market not open")
@pytest.mark.asyncio()
async def test_subscribe_bars_historical(self, client):
    callback_mock = Mock()

    client.bar_events += callback_mock

    contract = Contract()
    contract.conId = 553444806
    contract.exchange = "ICEEUSOFT"

    client.subscribe_bars(
        name="test",
        contract=contract,
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._15_SECOND,
        callback=callback_mock,
    )

    async def wait_for_bar():
        while callback_mock.call_count == 0:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_bar(), 2)

    assert callback_mock.call_count > 0


@pytest.mark.asyncio()
async def test_subscribe_bars_historical_returns_expected(self):
    def send_mocked_response(*args, **kwargs):
        bar = BarData()
        bar.date = 1700069390
        self.client.historicalDataUpdate(-10, bar)

    send_mock = Mock(side_effect=send_mocked_response)
    self.client._eclient.reqHistoricalData = send_mock

    callback_mock = Mock()

    # Act
    self.client.subscribe_bars(
        contract=Contract(),
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=callback_mock,
    )

    # Assert
    bar = callback_mock.call_args_list[0][0][0]
    assert isinstance(bar, BarData)
    assert bar.timestamp == pd.Timestamp("2023-11-15 17:29:50+00:00", tz="UTC")


@pytest.mark.asyncio()
async def test_unsubscribe_historical_bars(self):
    # Arrange
    self.client._eclient.reqHistoricalData = Mock()

    cancel_mock = Mock()
    self.client._eclient.cancelHistoricalData = cancel_mock

    # Act
    subscription = self.client.subscribe_bars(
        contract=Contract(),
        what_to_show=WhatToShow.BID,
        bar_size=BarSize._1_MINUTE,
        callback=Mock(),
    )
    subscription.cancel()

    # Assert
    cancel_mock.assert_called_once_with(reqId=-10)
    assert subscription not in self.client.subscriptions
