import json
import time
from pathlib import Path
from dotenv import dotenv_values

import requests

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

PROXY_EMAIL=dotenv_values()["PROXY_EMAIL"]
PROXY_PASSWORD=dotenv_values()["PROXY_PASSWORD"]

class ScrapeInstrumentHtml:
    def __init__(self):
        self._out_folder = Path(__file__).parent / Path(__file__).stem

    def scrape_all(self):
        path = Path(__file__).parent / "test_import_instruments.json"
        with open(path) as f:
            instruments: list[dict] = json.load(f)

        universe = IBTestProviderStubs.universe_dataframe()

        for symbol in universe.symbol:
            outpath = self._out_folder / f"{symbol}.html"

            if outpath.exists():
                print(f"File Exists {outpath} - Skipping...")
                continue

            # find the symbol in the instrument list
            matched = None
            for instrument in instruments:
                if instrument["ib_symbol"] == symbol:
                    matched = instrument
                    break
            assert matched is not None, f"Could not find symbol {symbol}"
            instrument = matched

            url = instrument["url"]

            print(f"Processing {url}")
            html = self.scrape(url)

            print(f"--> Writing {outpath}")
            outpath.parent.mkdir(parents=True, exist_ok=True)
            with open(outpath, "wb") as f:
                f.write(html)

            time.sleep(1)

    def scrape(self, url) -> bytes:
        """
        Scrapes an individual instrument page, eg:

        https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=54939868
        curl https://rayobyte.com/ -kx PROXY_EMAIL:PROXY_PASSWORD@la.residential.rayobyte.com:8000

        """
        print(f"Scraping {url}")
        proxies = {
            "http": f"{PROXY_EMAIL}:{PROXY_PASSWORD}@la.residential.rayobyte.com:8000",
            "https": f"{PROXY_EMAIL}:{PROXY_PASSWORD}@la.residential.rayobyte.com:8000",
        }
        from requests.auth import HTTPProxyAuth

        auth = HTTPProxyAuth(PROXY_EMAIL, PROXY_PASSWORD)
        
        r = requests.get(url, proxies=proxies, auth=auth)

        assert "To continue please enter the text from the image below" not in r.text
        return r.content

    
if __name__ == "__main__":
    url = "https://contract.ibkr.info/index.php?action=Details&site=GEN&conid=657796604"

    ScrapeInstrumentHtml().scrape_all()
    