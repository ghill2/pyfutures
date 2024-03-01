import json

from pytower import PACKAGE_ROOT

from pyfutures.schedule.factory import TIMEZONE_FOR_EXCHANGE
from pyfutures.tests.test_kit import IBTestProviderStubs


class TestExchangeCalendarFactory:
    def test_exchange_timezone_map(self):
        exchanges = list(TIMEZONE_FOR_EXCHANGE.keys())
        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            # TIMEZONE_FOR_EXCHANGE.get(instrument_id.venue.value)

            exchange = instrument_id.venue.value
            timezone = TIMEZONE_FOR_EXCHANGE.get(exchange)
            assert timezone is not None

            if exchange in exchanges:
                exchanges.remove(exchange)

        print(f"Unrequired: {exchanges}")

    def test_from_liquid_hours(self):
        MarketCalendarFactory.from_liquid_hours("MES")

    def test_find_missing_calendars(self):
        missing = set()

        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            path = MarketCalendarFactory.FOLDER / f"{instrument_id.symbol}.json"

            assert path.exists()

            # Open the JSON file for reading
            with open(path) as json_file:
                data = json.load(json_file)

            if data.get("Trading Hours") is None and data.get("Liquid Trading Hours") is None:
                missing.add(instrument_id)

        for item in missing:
            print(item)

    def test_from_symbol_universe(self):
        missing = set()
        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            result = MarketCalendarFactory.from_symbol(instrument_id.symbol)
            if result is None:
                missing.add(instrument_id.symbol)
        for symbol in missing:
            print(symbol)

    def test_from_market_hours_universe(self):
        missing = set()
        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            result = MarketCalendarFactory.from_market_hours(instrument_id.symbol)
            if result is False:
                missing.add(instrument_id)

        for item in missing:
            print(item)

    def test_from_liquid_hours_universe(self):
        missing = set()
        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            result = MarketCalendarFactory.from_liquid_hours(instrument_id.symbol)
            if result is False:
                missing.add(instrument_id)

        for instrument_id in missing:
            print(instrument_id)
            # instrument = UniverseProvider().find(instrument_id)
            # print(instrument.info['liquidHours'])

    def test_multiple_exchanges_universe(self):
        import json

        for instrument_id in IBTestProviderStubs.universe_instrument_ids():
            folder = PACKAGE_ROOT / "data/ib/instruments/info_universe_json"
            path = folder / f"{instrument_id.symbol}.json"
            with open(path) as json_file:
                json_data = json.load(json_file)
                if "," in json_data["Exchange"]:
                    print(instrument_id, json_data["Exchange"])
