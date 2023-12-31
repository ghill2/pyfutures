import time
from decimal import Decimal

import pandas as pd
import pytz
# from ibapi.common import UNSET_DECIMAL
# from ibapi.common import UNSET_DOUBLE
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order as IBOrder

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.enums import asset_class_from_str
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol

# from pyfutures.adapters.interactive_brokers.client import OrderEvent
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders.base import Order
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.continuous.contract_month import ContractMonth


def ib_quote_tick_to_nautilus_quote_tick(
    instrument: Instrument,
    tick: IBQuoteTick,
):
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=instrument.make_price(tick.bid_price),
        ask_price=instrument.make_price(tick.ask_price),
        bid_size=instrument.make_qty(tick.bid_size),
        ask_size=instrument.make_qty(tick.ask_size),
        ts_event=dt_to_unix_nanos(tick.time),
        ts_init=dt_to_unix_nanos(tick.time),
    )


def ib_bar_to_nautilus_bar(
    bar_type: BarType,
    bar: IBBar,
    instrument: Instrument,
    is_revision: bool = False,
) -> Bar:
    ts_init = (
        pd.Timestamp.fromtimestamp(int(bar.date), tz=pytz.utc).value
        + pd.Timedelta(bar_type.spec.timedelta).value
    )
    bar = Bar(
        bar_type=bar_type,
        open=instrument.make_price(bar.open),
        high=instrument.make_price(bar.high),
        low=instrument.make_price(bar.low),
        close=instrument.make_price(bar.close),
        volume=instrument.make_qty(0 if bar.volume == -1 else bar.volume),
        ts_event=dt_to_unix_nanos(bar.time),
        ts_init=ts_init,
        is_revision=is_revision,
    )

    return bar


def format_ib_timestamp(value: pd.Timestamp) -> str:
    return value.strftime("yyyyMMdd-HH:mm:ss")


order_side_to_order_action: dict[str, str] = {
    "BOT": "BUY",
    "SLD": "SELL",
}


def parse_datetime(value: str) -> pd.Timestamp:
    if isinstance(value, str):
        assert (
            len(value.split()) != 3
        ), f"""
            datetime value was {value}
            """

    if isinstance(value, int) or value.isdigit():
        return unix_nanos_to_dt(secs_to_nanos(int(value)))
    elif isinstance(value, str) and len(value) == 8:
        # YYYYmmdd
        # daily historical bars
        return pd.to_datetime(value, format="%Y%m%d", utc=True)
    elif isinstance(value, str) and len(value) == 17:
        return pd.to_datetime(value, format="%Y%m%d-%H:%M:%S", utc=True)

    raise RuntimeError("Unable to parse timestamp")


def contract_details_to_instrument_id(details: IBContractDetails) -> InstrumentId:
    return _format_instrument_id(
        symbol=details.contract.symbol,
        tradingClass=details.contract.tradingClass,
        exchange=details.contract.exchange,
        contractMonth=details.contractMonth,
    )


def contract_to_instrument_id(contract: IBContract) -> InstrumentId:
    assert len(contract.lastTradeDateOrContractMonth) == 6

    return _format_instrument_id(
        symbol=contract.symbol,
        tradingClass=contract.tradingClass,
        exchange=contract.exchange,
        contractMonth=contract.lastTradeDateOrContractMonth,
    )


def _format_instrument_id(
    symbol: str,
    tradingClass: str,
    exchange: str,
    contractMonth: str,
) -> InstrumentId:
    symbol = symbol.replace(".", "_")
    trading_class = tradingClass.replace(".", "_")
    exchange = exchange.replace(".", "_")
    month = str(ContractMonth.from_int(int(contractMonth)))
    return InstrumentId.from_str(f"{symbol}-{trading_class}={month}.{exchange}")


def instrument_id_to_contract(instrument_id: InstrumentId) -> IBContract:
    
    symbol, trading_class = tuple(instrument_id.symbol.value.split("=")[0].split("-"))
    exchange = instrument_id.venue.value

    contract = IBContract()

    contract.symbol = symbol.replace("_", ".")
    contract.exchange = exchange.replace("_", ".")
    contract.tradingClass = trading_class.replace("_", ".")
    
    if "=" in instrument_id.symbol.value:
        contract_month = ContractMonth(instrument_id.symbol.value.split("=")[1])
        contract.lastTradeDateOrContractMonth = str(contract_month.to_int())
        
    contract.includeExpired = False
    contract.secType = "FUT"

    return contract

def contract_id_to_contract(instrument_id: InstrumentId) -> IBContract:
    
    contract_month = instrument_id.symbol.value.split("=")[1]
    symbol, trading_class = tuple(instrument_id.symbol.value.split("=")[0].split("-"))
    exchange = instrument_id.venue.value

    contract = IBContract()

    contract.symbol = symbol.replace("_", ".")
    contract.exchange = exchange.replace("_", ".")
    contract.tradingClass = trading_class.replace("_", ".")
    contract.lastTradeDateOrContractMonth = str(ContractMonth.from_str(contract_month).to_int())
    contract.includeExpired = False
    contract.secType = "FUT"

    return contract

def contract_details_to_instrument(
    details: IBContractDetails,
    overrides: dict | None = None,
) -> FuturesContract:
    timestamp = time.time_ns()
    instrument_id = contract_details_to_instrument_id(details)

    min_tick = details.minTick * details.priceMagnifier
    price_precision = len(f"{min_tick:.8f}".rstrip("0").split(".")[1])
    price_increment = Price(min_tick, price_precision)

    return FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(details.contract.localSymbol),
        asset_class=_sec_type_to_asset_class(details.underSecType),
        currency=Currency.from_str(details.contract.currency),
        price_precision=overrides.get("price_precision") or price_precision,
        price_increment=overrides.get("price_increment") or price_increment,
        multiplier=overrides.get("multiplier")
        or Quantity.from_str(str(details.contract.multiplier)),
        lot_size=overrides.get("lot_size") or Quantity.from_int(1),
        underlying=details.underSymbol,
        activation_ns=0,
        expiration_ns=dt_to_unix_nanos(
            parse_datetime(details.contract.lastTradeDateOrContractMonth),
        ),
        ts_event=timestamp,
        ts_init=timestamp,
        info=contract_details_to_dict(details),
    )


def _sec_type_to_asset_class(sec_type: str):
    mapping = {
        "STK": "EQUITY",
        "IND": "INDEX",
        "CASH": "FX",
        "BOND": "BOND",
    }
    return asset_class_from_str(mapping.get(sec_type, sec_type))


def dict_to_contract(value: dict) -> IBContract:
    contract = IBContract()
    for key, value in value.items():
        setattr(contract, key, value)
    return contract


def dict_to_contract_details(value: dict) -> IBContractDetails:
    details = IBContractDetails()
    details.contract = dict_to_contract(value["contract"])
    value.pop("contract")
    for key, value in value.items():
        setattr(details, key, value)
    return details


def contract_details_to_dict(value: IBContractDetails) -> dict:
    data = {}
    data = data | value.__dict__.copy()
    data["contract"] = value.contract.__dict__.copy()
    return data


map_order_status = {
    "ApiPending": OrderStatus.SUBMITTED,
    "PendingSubmit": OrderStatus.SUBMITTED,
    "PendingCancel": OrderStatus.PENDING_CANCEL,
    "PreSubmitted": OrderStatus.SUBMITTED,
    "Submitted": OrderStatus.ACCEPTED,
    "ApiCancelled": OrderStatus.CANCELED,
    "Cancelled": OrderStatus.CANCELED,
    "Filled": OrderStatus.FILLED,
    "Inactive": OrderStatus.DENIED,
}
map_order_type: dict[int, str] = {
    OrderType.LIMIT: "LMT",
    OrderType.LIMIT_IF_TOUCHED: "LIT",
    OrderType.MARKET: "MKT",
    OrderType.MARKET_IF_TOUCHED: "MIT",
    OrderType.MARKET_TO_LIMIT: "MTL",
    OrderType.STOP_LIMIT: "STP LMT",
    OrderType.STOP_MARKET: "STP",
    OrderType.TRAILING_STOP_LIMIT: "TRAIL LIMIT",
    OrderType.TRAILING_STOP_MARKET: "TRAIL",
}
map_order_action: dict[int, str] = {
    OrderSide.BUY: "BUY",
    OrderSide.SELL: "SELL",
}

map_time_in_force: dict[int, str] = {
    TimeInForce.DAY: "DAY",
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.GTD: "GTD",
    TimeInForce.AT_THE_OPEN: "OPG",
    TimeInForce.FOK: "FOK",
    # unsupported: 'DTC',
}
map_trigger_method: dict[int, int] = {
    TriggerType.DEFAULT: 0,
    TriggerType.DOUBLE_BID_ASK: 1,
    TriggerType.LAST_TRADE: 2,
    TriggerType.DOUBLE_LAST: 3,
    TriggerType.BID_ASK: 4,
    TriggerType.LAST_OR_BID_ASK: 7,
    TriggerType.MID_POINT: 8,
}
ib_to_nautilus_trigger_method = dict(zip(map_trigger_method.values(), map_trigger_method.keys()))
ib_to_nautilus_time_in_force = dict(zip(map_time_in_force.values(), map_time_in_force.keys()))
ib_to_nautilus_order_side = dict(zip(map_order_action.values(), map_order_action.keys()))
ib_to_nautilus_order_type = dict(zip(map_order_type.values(), map_order_type.keys()))


def order_event_to_order_status_report(
    event: "OrderEvent",
    instrument: Instrument,
    now_ns: pd.Timestamp,
    account_id: AccountId,
) -> OrderStatusReport:
    # total_qty = (
    #     Quantity.from_int(0)
    #     if event.totalQuantity == UNSET_DECIMAL
    #     else Quantity.from_str(str(event.totalQuantity))
    # )
    # filled_qty = (
    #     Quantity.from_int(0)
    #     if event.filledQuantity == UNSET_DECIMAL
    #     else Quantity.from_str(str(event.filledQuantity))
    # )
    total_qty = Quantity.from_str(str(event.totalQuantity))
    filled_qty = Quantity.from_str(str(event.filledQuantity))
    if total_qty.as_double() > filled_qty.as_double() > 0:
        order_status = OrderStatus.PARTIALLY_FILLED
    else:
        order_status = map_order_status[event.status]

    # price = None if event.lmtPrice == UNSET_DOUBLE else instrument.make_price(event.lmtPrice)
    price = instrument.make_price(event.lmtPrice)

    order_status = OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(str(event.orderId)),
        order_side=ib_to_nautilus_order_side[event.action],
        order_type=ib_to_nautilus_order_type[event.orderType],
        time_in_force=ib_to_nautilus_time_in_force[event.tif],
        order_status=order_status,
        quantity=total_qty,
        filled_qty=Quantity.from_int(0),
        avg_px=Decimal(0),
        report_id=UUID4(),
        ts_accepted=now_ns,
        ts_last=now_ns,
        ts_init=now_ns,
        client_order_id=ClientOrderId(str(event.orderId)),
        price=price,
        # order_list_id=,
        # contingency_type=,
        # expire_time=expire_time,
        # trigger_price=instrument.make_price(order.auxPrice),
        # trigger_type=TriggerType.BID_ASK,
        # limit_offset=,
        # trailing_offset=,
    )

    return order_status


def nautilus_order_to_ib_order(order: Order, instrument: Instrument) -> IBOrder:
    ib_order = IBOrder()

    ib_order.orderId = int(order.client_order_id.value)
    ib_order.orderType = map_order_type[order.order_type]
    ib_order.totalQuantity = order.quantity.as_decimal()
    ib_order.action = map_order_action[order.side]
    ib_order.tif = map_time_in_force[order.time_in_force]

    if getattr(order, "price", None) is not None:
        ib_order.lmtPrice = order.price.as_double()

    if getattr(order, "display_qty", None) is not None:
        ib_order.displaySize = order.display_qty.as_double()

    if getattr(order, "expire_time", None) is not None:
        ib_order.goodTillDate = order.expire_time.strftime("%Y%m%d %H:%M:%S %Z")

    if getattr(order, "parent_order_id", None) is not None:
        ib_order.parentId = order.parent_order_id.value

    contract = IBContract()
    contract.conId = instrument.info["contract"]["conId"]
    contract.exchange = instrument.info["contract"]["exchange"]
    ib_order.contract = contract

    return ib_order

