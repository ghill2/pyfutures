import json
import time
from pathlib import Path
from dotenv import dotenv_values
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.proxy import Proxy, ProxyType
import requests

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

PROXY_EMAIL = dotenv_values()["PROXY_EMAIL"]
PROXY_PASSWORD = dotenv_values()["PROXY_PASSWORD"]

"""
Scrapes the html of instruments in the universe
quote_currency
multiplier
trading hours
liquid trading hours
"""


class ScrapeInstrumentHtml:
    def __init__(self):
        self._out_folder = Path(__file__).parent / Path(__file__).stem

    def scrape_all(self):
        universe = IBTestProviderStubs.universe_dataframe()

        import geckodriver_autoinstaller
        from selenium import webdriver

        geckodriver_autoinstaller.install()  # Check if the current version of geckodriver exists
        # and if it doesn't exist, download it automatically,
        # then add geckodriver to path
        from geckodriver_autoinstaller.utils import get_geckodriver_path

        # from geckodriver_autoinstaller import get_geckodriver_filename
        # Initialize Firefox WebDriver with proxy settings
        # "http":
        # "https": f"{PROXY_EMAIL}:{PROXY_PASSWORD}@la.residential.rayobyte.com:8000",
        proxy = Proxy(
            {
                "proxyType": ProxyType.MANUAL,
                "httpProxy": f"{PROXY_EMAIL}:{PROXY_PASSWORD}@la.residential.rayobyte.com:8000",
                "ftpProxy": "proxy_server_address:port",
                "sslProxy": "proxy_server_address:port",
                "noProxy": "",  # Optional: specify domains that should bypass the proxy, e.g., 'localhost,127.0.0.1'
            }
        )

        firefox_options = webdriver.FirefoxOptions()
        firefox_options.headless = False  # Set to True if you want to run Firefox in headless mode

        # firefox_profile = webdriver.FirefoxProfile()
        firefox_options.set_preference("network.proxy.type", 1)  # Manual proxy configuration
        firefox_options.set_preference("network.proxy.http", "la.residential.rayobyte.com")
        firefox_options.set_preference("network.proxy.http_port", 8000)
        firefox_options.set_preference("network.proxy.socks_username", PROXY_EMAIL)
        firefox_options.set_preference("network.proxy.socks_password", PROXY_PASSWORD)

        from selenium.webdriver.firefox.service import Service

        driver = webdriver.Firefox(
            service=Service(
                executable_path=".venv/lib/python3.10/site-packages/geckodriver_autoinstaller/v0.34.0/geckodriver",
            ),
            options=firefox_options,
        )
        url = "https://contract.ibkr.info/v3.10/index.php?action=Details&site=GEN&conid=54939868"

        for row in universe.itertuples():
            outpath = self._out_folder / f"{row.trading_class}.html"

            # if outpath.exists():
            #     print(f"File Exists {outpath} - Skipping...")
            #     continue

            print(f"Processing {url}")
            driver.get(row.url)
            html_content = driver.page_source

            print(f"--> Writing {outpath}")
            outpath.parent.mkdir(parents=True, exist_ok=True)
            with open(outpath, "w") as f:
                f.write(html_content)

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
        print(r.content)
        assert "To continue please enter the text from the image below" not in r.text
        return r.content


if __name__ == "__main__":
    url = "https://contract.ibkr.info/index.php?action=Details&site=GEN&conid=657796604"
    ScrapeInstrumentHtml().scrape_all()

    # # assert "Python" in driver.title
    # # exit()
    # # from selenium import webdriver

    # # # Path to the Chrome WebDriver executable
    # # # driver_path = '/Users/g1/BU/projects/pytower_develop/.venv/lib/python3.10/site-packages/chromedriver_py/chromedriver_mac-x64'
    # # from chromedriver_py import binary_path

    # # options = ChromeOptions()
    # # options.binary_location = binary_path
    # # options.headless = True
    # # options.add_argument("start-maximized"); # open Browser in maximized mode
    # # options.add_argument("--disable-extensions") # disabling extensions
    # # options.add_argument('--disable-logging')
    # # options.add_argument('--remote-debugging-pipe')  # fix port error
    # # options.add_argument("--disable-dev-shm-usage"); # overcome limited resource problems
    # # options.add_argument("--no-sandbox"); # Bypass OS security model
    # # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # # options.add_argument("--disable-browser-side-navigation"); # https://stackoverflow.com/a/49123152/1689770
    # # options.add_argument("--window-size=1920,1080")

    # # driver = webdriver.Chrome(
    # #     service=Service(executable_path=binary_path),
    # #     options=options
    # # )

    # # Open a website
    # # driver.get('www.google.com')

    # exit()
