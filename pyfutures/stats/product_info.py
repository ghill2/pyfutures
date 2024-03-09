import functools
import pickle
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pyfutures.stats.fx_rates import FxRates
from pyfutures.tests.test_kit import IBTestProviderStubs


def cache_pickle_daily(dir, filename):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            today = datetime.utcnow().strftime("%Y-%m-%d")
            path = (
                Path.home()
                / "Desktop"
                / "pyfutures_cache"
                / dir
                / today
                / f"{filename}.pkl"
            )

            if path.exists():
                print(f"Loading {filename} from cache - {path}")
                with open(path, "rb") as f:
                    return pickle.load(f)

            result = func(*args, **kwargs)

            print(f"Caching {filename} - {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            pickle.dump(result, open(path, "wb"))

            return result

        return wrapper

    return decorator


# def cache_text_daily(cachedir, dir: str, name, extension):
#     def decorator(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             today = datetime.utcnow().strftime("%Y-%m-%d")
#             path = cachedir / today / f"{name}.pkl"
#
#             if path.exists():
#                 print(f"Loading {name} from cache - {path}")
#                 with open(path, "rb") as f:
#                     return pickle.load(f)
#
#             result = func(*args, **kwargs)
#
#             print(f"Caching {name} - {path}")
#             pickle.dump(result, open(path, "wb"))
#
#             return result
#
#         return wrapper
#
#     return decorator
#
#
def scrape_tables(html_content):
    """
    Scrapes all tables in the provided HTML content and returns a list of dictionaries.

    Args:
    ----
        html_content (str): The HTML content to scrape.

    Returns:
    -------
        list: A list of dictionaries, where each dictionary represents a table row with keys
              corresponding to the column headers.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")

    scraped_data = []
    for table in tables:
        header_row = table.find("tr")  # Assuming first row is header

        if header_row:  # Check if header row exists
            column_headers = [
                th.text.strip() for th in header_row.find_all(["td", "th"])
            ]
        else:
            # Handle case where there's no header row
            column_headers = [
                f"cell_{i+1}"
                for i in range(len(table.find_all("tr")[1].find_all(["td", "th"])))
            ]

        rows = table.find_all("tr")[1:]  # Skip the header row
        for row in rows:
            table_dict = {}
            cells = row.find_all(["td", "th"])
            for i, cell in enumerate(cells):
                table_dict[column_headers[i]] = cell.text.strip()
            scraped_data.append(table_dict)
    return scraped_data


class MarginInfo:
    """
    Scrapes all Future Instrument margin pages from this root url:
    https://www.interactivebrokers.co.uk/en/trading/margin-requirements.php#margin-matrix

    Each link in this format:
    https://www.interactivebrokers.co.uk/en/index.php?f=40539&hm=uk&ex={REGION}&rgt=0&rsk=1&pm=0&rst=010404040101010808
    """

    regions = ["us", "ca", "eu", "au", "hk", "jp", "in", "mx", "sg", "kr"]

    def __init__(self):
        self.margin_instruments = None
        self.margin_rows = None
        self.fx_rates = FxRates()

    def _scrape(self, region: str):
        """
        Scrapes a single margin info page
        cached is updated daily upon execution
        """
        url = self.url(region)
        today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
        outpath = self.cachedir / today / f"{region}.html"
        outpath.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(outpath) as f:
                html = f.read()
        except FileNotFoundError:
            print("-----> Getting Margin Info from Web...")
            response = requests.get(url)
            response.raise_for_status()
            with open(outpath, "wb") as f:
                f.write(response.content)
            return response.content
        else:
            print("-----> Getting Margin Info from Disk...")
            return html

    def _validate(self, result):
        """
        validates a row of the table
        """
        exchange_key = None
        intraday_initial_key = None
        for k, v in result.items():
            if k.startswith("Exchange "):
                exchange_key = k
            if k.startswith("Intraday Initial "):
                intraday_initial_key = k

        if exchange_key is not None:
            result["Exchange"] = result.pop(exchange_key)  # rename
        if intraday_initial_key is not None:
            result["Intraday Initial"] = result.pop(intraday_initial_key)  # rename

        # some dicts do not have a trading class
        result.setdefault("Trading Class", None)

        return result

    def _parse(self, html):
        """ """
        results = scrape_tables(html)
        results = [self._validate(r) for r in results]
        return results

    @cache_pickle_daily(dir="margin_info", filename="margin_info")
    def get_all(self):
        """
        Get results from margin info pages
        """
        all_instruments = []
        for region in self.regions:
            html = self._scrape(region)
            instruments = self._parse(html)
            all_instruments.extend(instruments)

        return all_instruments

    @staticmethod
    def _find(m_instruments, row):
        """
        finds the margin instrument for the given UniverseRow
        """
        for m_inst in m_instruments:
            if (
                m_inst["Exchange"] == row.exchange.replace(",", ".")
                and m_inst["Trading Class"] == row.trading_class
            ):
                return m_inst

        raise ValueError(f"UniverseRow not found in margin info instruments: {row}")

    @cache_pickle_daily(dir="margin_info", filename="margin_rows")
    def get_margin_rows(self):
        """
        Get all margin instruments for instruments in the universe, with the margin converted to GBP
        margin rows = universe margin instruments / rows of the tables from the scrape that are in the universe
        TODO: this can be merged with get_all()
        """
        if self.margin_instruments is None:
            self.margin_instruments = self.get_all()

        # find all universe margin instruments
        margin_rows = [
            self._find(self.margin_instruments, r)
            for r in IBTestProviderStubs.universe_rows()
        ]
        # convert margin values in non GBP currency to GBP
        currencies = set([i["Currency"] + "GBP" for i in margin_rows])
        fx_rates = self.fx_rates.get(currencies)
        m_rows = []
        for m_row in margin_rows:
            fx_rate = fx_rates[m_row["Currency"] + "GBP"]
            margin_value = float(m_row["Intraday Initial"]) * fx_rate
            m_rows.append(
                {
                    "Exchange": m_row["Exchange"],
                    "Trading Class": m_row["Trading Class"],
                    "value": margin_value,
                }
            )

        return sorted(m_rows, key=lambda x: x["value"])

    def sort_by_margin(self, rows):
        """
        Sorts List[UniverseRow] by their margin values
        Usage: MarginInfo().sort_by_margin(IBTestProviderStubs.universe_rows())
        """
        if self.margin_rows is None:
            self.margin_rows = self.get_margin_rows()

        def custom_sort(obj):
            m_row = self._find(self.margin_rows, obj)
            return m_row["value"]

        return sorted(rows, key=custom_sort)

    @staticmethod
    def url(region):
        """
        generate url for the region
        """
        return f"https://www.interactivebrokers.co.uk/en/index.php?f=40539&hm=uk&ex={region}&rgt=0&rsk=1&pm=0&rst=010404040101010808"


class ProductInfo:
    """
    Margin values are not available using the API
    they have to be retrieved from the product page for the instrument
    As values listed on the product pages are
    """

    def product_Pages(self, exchange, symbol, url):
        """
        downloads closing price from exchange page
        saves the html file to disk to reduce CAPTCHA amount on reruns (proxy not yet implemented)
        originally from stats,

        """
        outpath = self.parent_out / "exchange_info" / f"{exchange}-{symbol}.html"
        outpath.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(outpath) as f:
                html = f.read()
        except FileNotFoundError:
            print("-----> Getting Contract Price from Web...")
            response = requests.get(url)
            # Raise an exception for non-200 status codes
            response.raise_for_status()
            with open(outpath, "wb") as f:
                f.write(response.content)
            html = response.content
            print(f"Downloaded and saved HTML content from {url} to {outpath}")
        else:
            print("-----> Getting Contract Price from File...")
        soup = BeautifulSoup(html, "html.parser")
        close_price = soup.find(string="Closing Price").find_next("td").text
        print("product page close price: ", close_price)
        # try:
        if close_price == "n/a":
            return None
        close_price = float(close_price)
        # except Exception as e:
        # print(e)
        # return None
        return close_price

    def get(since: pd.Timestamp = None):
        """
        Get the Product Page for the instrument
        Checks on the filesystem, if not found it downloads using requests
        since: use product pages
        """
