import joblib
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import BarAggregation
from pyfutures.tests.test_kit import MULTIPLE_PRICES_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs
from nautilus_trader.continuous.wranglers import ContinuousBarWrangler
from nautilus_trader.continuous.contract_month import ContractMonth

def validate(row: dict) -> None:
    bars = row.contract_bars
    
    letter_month = row.chain_config.roll_config.hold_cycle.value[-1]
    end_month = ContractMonth(f"2023{letter_month}")
    
    print(f"Validating {row.trading_class}: end_month={end_month} {len(bars)} bars...")
    
    wrangler = ContinuousBarWrangler(
        config=row.chain_config,
        end_month=end_month,
    )
    wrangler.validate(bars)
    
if __name__ == "__main__":
    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
        # skip=[
        #     "EBM",
        # ],
    )

    results = joblib.Parallel(n_jobs=-1, backend="loky")(joblib.delayed(validate)(row) for row in rows)

    # # need carry price bars too
    # month = ContractMonth(path.stem.split("=")[1].split(".")[0])
    # if month in (wrangler.chain.hold_cycle) or (month in wrangler.chain.priced_cycle):

    # min_ = find_minimum_day_of_month_within_range(
    #     start=ContractMonth("1999H").timestamp_utc,
    #     end=ContractMonth("2025H").timestamp_utc,
    #     dayname="Thursday",
    #     dayofmonth=2,
    # )

    # def add_minute_missing_bars(trading_class: str, bars: list[Bar]) -> list[Bar]:
    # # minute missing bars

    # # add extra bars
    # if trading_class == "FESB":
    #     "FESB 2007U - 20070615, 1621, 480.00, 482,00, 479.30, 482.00, 1, 20"

    #     bars.append(
    #         Bar(
    #             BarType.from_str("FESB_SX7E=2007U.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("480.00"),
    #             high=Price.from_str("482.00"),
    #             low=Price.from_str("479.30"),
    #             close=Price.from_str("482.00"),
    #             volume=Quantity.from_str("20.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
    #         )
    #     )

    # # data[ContractMonth("2007U")] = [bar] + data[ContractMonth("2007U")]
    # elif trading_class == "FESU":
    #     """
    #     20191221, time 282.40, 283.80, 280.90, 283.80, 1, 13
    #     20191227, time, 280.00, 280.60, 273.40, 275.20, 1, 10
    #     2018-12-21 17:53:00
    #     """
    #     bars.extend([
    #         Bar(
    #             BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("282.40"),
    #             high=Price.from_str("283.80"),
    #             low=Price.from_str("280.90"),
    #             close=Price.from_str("283.80"),
    #             volume=Quantity.from_str("13.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
    #         ),
    #         Bar(
    #             BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("280.00"),
    #             high=Price.from_str("280.60"),
    #             low=Price.from_str("273.40"),
    #             close=Price.from_str("275.20"),
    #             volume=Quantity.from_str("10.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
    #         ),
    #     ])
    # return bars

    # def import_missing_ebm_months():

#     # load minute EBM bars from years 2013X, 2014F, 2014X, 2015F
#     rows = IBTestProviderStubs.universe_rows(filter=["EBM"])
#     assert len(rows) == 1
#     row = rows[0]

#     months = ("2013X", "2014F", "2014X", "2015F")

#     bars_list = []
#     for month in months:

#         files = IBTestProviderStubs.bar_files(
#             "EBM",
#             BarAggregation.MINUTE,
#             month=month,
#         )

#         assert len(files) == 1
#         file = files[0]

#         df = file.read(
#             timestamp_delta=(row.settlement_time, row.timezone),
#             to_aggregation=(1, BarAggregation.DAY),
#         )

#         bar_type = BarType.from_str(f"EBM_EBM={month}.IB-1-DAY-MID-EXTERNAL")
#         wrangler = BarDataWrangler(
#             bar_type=bar_type,
#             instrument=row.base,
#         )
#         bars = wrangler.process(data=df)
#         bars_list.extend(bars)

#     return bars_list
# def add_missing_daily_bars(trading_class: str, bars: list[Bar]) -> list[Bar]:

#     # add extra bars
#     if trading_class == "FESU":

#         data = (
#             ("2018Z", "2018-12-17", 290.6),
#             ("2018Z", "2018-12-18", 286.4),
#             ("2018Z", "2018-12-19", 289.7),
#             ("2018Z", "2018-12-20", 287.3),
#             ("2018Z", "2018-12-21", 284.7),

#             ("2019H", "2018-12-14", 287.5),
#             ("2019H", "2018-12-17", 287.6),
#             ("2019H", "2018-12-18", 283.4),
#             ("2019H", "2018-12-19", 286.7),
#             ("2019H", "2018-12-20", 284.3),
#             ("2019H", "2018-12-21", 283.8),
#             ("2019H", "2018-12-27", 275.2),
#         )
#         for item in data:
#             contract, timestamp, close = item

#             timestamp_ns = dt_to_unix_nanos(pd.Timestamp(timestamp, tz="UTC"))
#             bars.append(
#                 Bar(
#                     BarType.from_str(f"FESU_ESU={contract}.IB-1-DAY-MID-EXTERNAL"),
#                     open=Price.from_str(str(close)),
#                     high=Price.from_str(str(close)),
#                     low=Price.from_str(str(close)),
#                     close=Price.from_str(str(close)),
#                     volume=Quantity.from_str("20.0"),
#                     ts_init=timestamp_ns,
#                     ts_event=timestamp_ns,
#                 ),
#             )
#     elif trading_class == "NIFTY":
#         data = """
#         20071227,6120.0,6120.0,6120.0,6120.0,5000,100000
#         20071228,6119.5,6119.5,6119.5,6119.5,5000,100000
#         20071231,6155.0,6155.0,6155.0,6155.0,5000,100000
#         20080101,6156.5,6156.5,6156.5,6156.5,5000,100000
#         20080102,6223.0,6223.0,6223.0,6223.0,5000,100000
#         20080103,6178.0,6178.0,6178.0,6178.0,5000,100000
#         20080104,6145.0,6289.0,6145.0,6255.0,4596,106508
#         20080107,6139.5,6288.0,6139.5,6288.0,4720,113831
#         20080108,6279.0,6320.0,6195.0,6269.0,6614,120547
#         20080109,6250.5,6318.0,6200.0,6260.0,2668,129891
#         20080110,6265.0,6312.0,6112.5,6162.0,3052,131973
#         20080111,6149.0,6235.0,6095.0,6222.0,5011,133297
#         20080114,6200.0,6225.0,6160.0,6225.0,2059,139088
#         20080115,6250.0,6250.0,6040.0,6058.0,4067,156727
#         20080116,6000.0,6030.0,5800.0,5947.0,16582,187309
#         20080117,5861.0,6035.0,5810.0,5922.0,5594,181865
#         20080118,5860.0,5918.5,5680.0,5720.0,11137,189895
#         20080121,5600.0,5615.0,4850.0,5198.0,19482,204858
#         20080122,4900.5,5098.0,4419.5,4920.0,21660,196269
#         20080123,5000.0,5340.0,4940.0,5164.0,27372,197447
#         20080124,5041.0,5370.0,4951.0,5001.0,14343,188951
#         20080125,5025.0,5404.0,4951.0,5404.0,9689,180177
#         20080128,5240.0,5277.0,5040.0,5251.5,8483,170850
#         20080129,5250.0,5360.0,5200.0,5277.0,6827,145856
#         20080130,5280.0,5380.0,5100.0,5167.0,6940,92295
#         20080131,5150.0,5351.0,5056.0,5138.0,0,0
#         """

#         for line in data.strip().splitlines():
#             items = line.strip().split(",")
#             timestamp_ns = dt_to_unix_nanos(
#                 pd.to_datetime(
#                     items[0],
#                     format="%Y%m%d",
#                     utc=True,
#                 )
#             )
#             bars.append(
#                 Bar(
#                     BarType.from_str("NIFTY_NIFTY50=2008F.IB-1-DAY-MID-EXTERNAL"),
#                     open=Price.from_str(items[1]),
#                     high=Price.from_str(items[2]),
#                     low=Price.from_str(items[3]),
#                     close=Price.from_str(items[4]),
#                     volume=Quantity.from_str(items[6]),
#                     ts_init=timestamp_ns,
#                     ts_event=timestamp_ns,
#                 ),
#             )

#     return bars

# bar_types = [data.bar_type for data in continuous_data]
    # assert len(bar_types) == 3

    # # write prices parquet and csv
    # for data in continuous_data:
    #     aggregation = data.bar_type.spec.aggregation
    #     file = files[aggregation]
    #     path = str(file.path)
    #     writer = MultipleBarParquetWriter(path=path)

    #     prices_ = prices[data.bar_type]

    #     end_year = prices_[-1].current_month.year
    #     if end_year != 2023:
    #         print(f"data.prices[-1].current_month.year != 2023 {row.symbol}")
    #         raise RuntimeError

    #     print(f"Writing {bar_aggregation_to_str(aggregation)} prices... " f"{len(prices_)} items {path}")
    #     # write parquet
    #     writer.write_objects(data=prices_)

        # # write csv
        # path = file.path.with_suffix(".csv")
        # df = MultiplePriceParquetWriter.to_table(data=prices_).to_pandas()
        # df["timestamp"] = df.ts_event.apply(unix_nanos_to_dt)
        # df.to_csv(path, index=False)

# def write(row: dict, skip: bool = True, debug: bool = False) -> None: