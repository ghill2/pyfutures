import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup


class ProductInfo:
    """
    Margin values are not available using the API
    they have to be retrieved from the product page for the instrument
    As values listed on the product pages are
    """

    pass

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
        pass


class MarginInfo:
    """
    Scrapes all Future Instrument margin pages from this root url:
    https://www.interactivebrokers.co.uk/en/trading/margin-requirements.php#margin-matrix

    Each link in this format:
    https://www.interactivebrokers.co.uk/en/index.php?f=40539&hm=uk&ex={REGION}&rgt=0&rsk=1&pm=0&rst=010404040101010808
    """

    regions = ["us", "ca", "eu", "au", "hk", "jp", "in", "mx", "sg", "kr"]
    cachedir = Path.home() / "Desktop" / "pyfutures_cache"

    def _scrape(self, region: str):
        url = self.url(region)
        try:
            response = requests.get(url)
            response.raise_for_status()
        except FileNotFoundError:
            pass

    def _scrape_all(self):
        for region in self.regions:
            self.scrape(region=region)

    def _parse(self):
        pass

    # def parse_all(self):
    # pass

    def get_all(self):
        for region in self.regions:
            self.scrape(region)
            break

    @staticmethod
    def url(region):
        """
        generate url for the region
        """
        return f"https://www.interactivebrokers.co.uk/en/index.php?f=40539&hm=uk&ex={region}&rgt=0&rsk=1&pm=0&rst=010404040101010808"
