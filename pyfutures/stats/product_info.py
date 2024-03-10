from datetime import timedelta

from requests_cache import CachedSession


class ProductInfo:
    """ """

    def __init__(self):
        self.session = CachedSession(
            "product_info",
            use_cache_dir=True,  # Save files in the default user cache dir
            cache_control=True,  # Use Cache-Control response headers for expiration, if available
            expire_after=timedelta(days=1),  # Otherwise expire responses after one day
            allowable_codes=[200],  # Do not cache 400
            allowable_methods=["GET", "POST"],  # Cache whatever HTTP methods you want
            ignored_parameters=["api_key"],  # Don't match this request param, and redact if from the cache
            match_headers=["Accept-Language"],  # Cache a different response per language
            stale_if_error=True,  # In case of request errors, use stale cache data if possible
        )

    # def download_roduct_Pages(self, exchange, symbol, url):
    #     """
    #     downloads closing price from exchange page
    #     saves the html file to disk to reduce CAPTCHA amount on reruns (proxy not yet implemented)
    #     originally from stats,
    #
    #     """
    #     outpath = self.parent_out / "exchange_info" / f"{exchange}-{symbol}.html"
    #     outpath.parent.mkdir(parents=True, exist_ok=True)
    #     try:
    #         with open(outpath) as f:
    #             html = f.read()
    #     except FileNotFoundError:
    #         print("-----> Getting Contract Price from Web...")
    #         response = requests.get(url)
    #         # Raise an exception for non-200 status codes
    #         response.raise_for_status()
    #         with open(outpath, "wb") as f:
    #             f.write(response.content)
    #         html = response.content
    #         print(f"Downloaded and saved HTML content from {url} to {outpath}")
    #     else:
    #         print("-----> Getting Contract Price from File...")
    #     soup = BeautifulSoup(html, "html.parser")
    #     close_price = soup.find(string="Closing Price").find_next("td").text
    #     print("product page close price: ", close_price)
    #     # try:
    #     if close_price == "n/a":
    #         return None
    #     close_price = float(close_price)
    #     # except Exception as e:
    #     # print(e)
    #     # return None
    #     return close_price

    def download(self, url):
        response = self.session.get(url)
        response.raise_for_status()
        html = response.content
        return html
