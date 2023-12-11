import json
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URLS = {
    "CFE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=644107697",
    "CME": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=355341657",
    "CBOT": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=657138520",
    "COMEX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=656780469",
    "NYMEX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=421710646",
    "NYBOT": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=634760278",
    "ICEUS": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=655347572",
    "NYSELIFFE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=638175029",
    "CDE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=476937773",
    "MEXDER": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=638254045",
    "BELFOX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=644561738",
    "MATIF": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=653550121",
    "MONEP": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=347388076",
    "EUREX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=655096186",
    "IDEM": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=644577202",
    "FTA": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=644559219",
    "ENDEX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=649974300",
    "MEFFRV": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=372590373",
    "OMS": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=636374656",
    "IPE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=346062665",
    "ICEEU": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=397149200",
    "ICEEUSOFT": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=631297410",
    "LMEOTC": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=657755348",
    "SNFE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=594944032",
    "HKFE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=645676809",
    "NSE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=652056080",
    "OSE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=637677715",
    "SGX": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=649385759",
    "KSE": "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=654500848",
}

class IBInstrumentsScraper:
    """
    Parses instrument information from interactivebrokers.co.uk into one JSON file.

    root_url:
        https://www.interactivebrokers.co.uk/en/index.php?f=41307&p=fut

    exchange_url:
        https://interactivebrokers.co.uk/en/index.php?f=41295&exch=hkfe&showcategories=FUTGRP

    """

    def __init__(self):
        self.base_url = "https://www.interactivebrokers.co.uk/en"

    def write_instruments(self):
        instruments = []
        instruments.extend(self.scrape_instruments())
        instruments.extend(self.scrape_currency_pairs())
        outpath = Path(__file__).parent / f"{Path(__file__).stem}.json"
        print(f"Data written to: {outpath}")
        with open(outpath, "w") as f:
            json.dump(instruments, f, indent=4)

    # ======================== WEB SCRAPER METHODS ========================
    def scrape_instruments(self, exchange_urls=None):
        """
        Get all future instruments from IB Product Listings into json file.
        """
        if exchange_urls is None:
            exchange_urls = self._exchange_urls()
            print(exchange_urls)

        # NEW: This writes list[dict] of all instrument and exchange combinations to a file
        instruments = []
        for region, ex_urls in exchange_urls.items():
            print("==================================================")
            for url in ex_urls:
                print(url)
                insts = self.parse_exchange_page(url)
                for inst in insts:
                    inst["region"] = region
                    inst["exchange"] = url.split("exch=")[1].split("&")[0].upper()
                instruments.extend(insts)
                if len(insts) > 100:
                    print(f"""url {url} contained multiple pages...""")

        return instruments

    def scrape_currency_pairs(self):
        """
        Download all currencies from the only currency exchange IDEALPRO into json file.
        """
        instruments = self.parse_exchange_page(
            "https://www.interactivebrokers.com/en/index.php?f=2222&exch=ibfxpro&showcategories=FX",
        )
        for inst in instruments:
            inst["exchange"] = "IDEALPRO"
            # TWS API uses X as symbol, even though the symbol on the product listings are X.Y
            inst["ib_symbol"] = inst["ib_symbol"].split(".")[0]
        return instruments

    # ======================== INSTRUMENT WEB SCRAPER CLIENT ========================

    def _exchange_urls(self):
        """
        returns list of all exchange URL pages
        https://www.interactivebrokers.co.uk/en/index.php?f=41307&p=fut
        NA + Europe + Asia
        -> Supports other security types but currently unused
        """
        data = {}
        regions = [
            {"param": "fut", "region": "NA"},
            {"param": "europe_fut", "region": "EUROPE"},
            {"param": "asia_fut", "region": "ASIA"},
        ]
        for region in regions:
            param = region["param"]
            url = f"{self.base_url}/index.php?f=41307&p={param}"
            response = requests.get(url)
            print(f"Scraping exchanges for url: {url}")
            soup = BeautifulSoup(response.content, "html.parser")
            urls = [self.base_url + "/" + link.get("href") for link in soup.select("table a")]
            # data.extend([dict(region=region["region"], url=url) for url in urls])
            data[region["region"]] = urls
        return data

    def extract_url(self, text):
        """
        Extract a URL from a string.
        """
        regex = r"(https?://[\w\.]+)"
        match = re.search(regex, text)

        if match:
            return match.group(1)
        else:
            return None

    def parse_table(self, table):
        """
        Bs4 table -> list[dict]
        """
        # Iterate over the rows of the table
        rows = table.select("tbody")[0].find_all("tr")

        # Create an empty list to store the rows
        rows_as_dicts = []
        column_names = ["ib_symbol", "description", "symbol", "currency"]

        # Iterate over the rows
        for row in rows:
            # Create an empty dictionary
            row_data = {}

            row_data["url"] = (
                row.select("a")[0].get("href").replace("javascript:NewWindow('", "").split("'")[0]
            )

            # Iterate over the cells of the row
            cells = row.find_all("td")

            # Add the text content of each cell to the dictionary
            for i, cell in enumerate(cells):
                row_data[column_names[i]] = cell.text

            # Add the row dictionary to the list of rows
            rows_as_dicts.append(row_data)

        return [i for i in rows_as_dicts if i]  # filter empty dicts from list[dict]

    def parse_exchange_page(self, base_url):
        """
        Returns a generator.
        """
        current_page = 1
        all_page_results = []
        while True:
            url = f"{base_url}&page={current_page}"
            print(f"Parsing url: {url}")
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            tables = soup.select("table")
            for table in tables:
                # there are 3 tables on each exchange page
                # choose the correct one to parse based on column table headers
                tcols = [th.get_text() for th in table.select("th")]
                if "IB Symbol" in tcols and "Symbol" in tcols and "Currency" in tcols:
                    results = self.parse_table(table)
                    # if len(results) < 99, the final page has been reached

                    all_page_results.extend(results)

                    if len(results) < 100:
                        return all_page_results

            current_page += 1

