# import requests
import json
import pprint
from pathlib import Path

from bs4 import BeautifulSoup
from bs4 import Tag

from pyfutures.data.ib.exceptions import ScraperDownloadFailedError


def get_all_text_recursive(element):
    """
    Returns all text recursively within the given element.

    Args:
    ----
      element: A BeautifulSoup element.

    Returns:
    -------
      A string containing all text recursively within the given element.

    """
    text = element.text
    for child in element.children:
        text += get_all_text_recursive(child)
    return text


class ImportInstrumentJson:
    def __init__(self):
        self._out_folder = Path(__file__).parent / Path(__file__).stem

    def import_all(self, write: bool = True):
        path = Path(__file__).parent / "scrape_instrument_html"
        html_files = list(path.rglob("*.html"))

        for path in html_files:
            data = self.parse_html_file(path=path)

            if write:
                outpath = self._out_folder / f"{path.stem}.json"
                with open(outpath, "w") as f:
                    json.dump(data, f, indent=4)

    def parse_html_file(self, path: Path):
        path = Path(path)
        print(f"Parsing {path}")

        with open(path) as f:
            try:
                data = self.parse_data(html_data=f.read())
            except ValueError as e:
                raise e
            else:
                return data

    def parse_data(self, html_data):
        soup = BeautifulSoup(html_data, "html.parser")
        tables = soup.select("#contractSpecs > table")  # only get first child tables

        data = {}
        if len(tables) > 1:
            for table in tables:
                self._scrape_table(data, table)
        else:
            print(f"Invalid Scraper HTML {html_data}")
            exit()

        # redownload
        if data["Underlying Information"]["Description/Name"] == "(@)":
            raise ScraperDownloadFailedError(f"Failed Instrument: {data}")

        data = self.validate(data)

        return data

    # ===================== IBParser Client =======================

    def _validate_trading_hours(self, hours_to_validate: object):
        hours = {}
        for k, day_hours in hours_to_validate.items():
            if len(day_hours) > 11:
                n = 11
                # split string at every N character
                day_hours = [day_hours[i * n : i * n + n] for i, blah in enumerate(day_hours[::n])]
                hours[k] = day_hours
            else:
                hours[k] = [day_hours]
        return hours

    def _validate_margin(self, v):
        if v != "Default":
            v = int(v.replace(",", ""))
        return v

    def validate(self, data):
        """
        Validate the data before writing int conversion.
        """
        if "IB Forex PRO (IDEALPRO) Top" in data:
            # dont validate currency pairs, not implemented
            return data

        del data["Underlying Information"]

        exchange = data["Contract Information"]["Exchange"].split(", ")
        exchange = exchange[0]

        d = {}

        # find the exchange info key name
        for key in data.keys():
            if key.endswith("Top"):
                # add the correct exchange info
                for prop, v in data[key].items():
                    d.setdefault(prop, v)

        # the only secondary exchanges listed and removed below are QBALGO, QBALGOIUS
        d["Exchange"] = exchange

        # delete all exchange info keys, we now have that data
        keys_to_delete = []
        for key in data:
            if key.endswith("Top"):
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del data[key]

        # flatten remaining
        for v in data.values():
            for k, vv in v.items():
                # raise ValueError(f"property already exists, {k}")
                d.setdefault(k, vv)

        # if "Liquid Trading Hours" not in d:
        #     print("==========")
        #     print(d["Exchange"], d["Symbol"])
        #     pprint.pprint(data)

        if "Trading Hours" in d:
            d["Trading Hours"] = self._validate_trading_hours(d["Trading Hours"])
        if "Liquid Trading Hours" in d:
            d["Liquid Trading Hours"] = self._validate_trading_hours(d["Liquid Trading Hours"])

        for k in [
            "Overnight Initial Margin",
            "Overnight Maintenance Margin",
            "Intraday Initial Margin",
            "Intraday Maintenance Margin",
        ]:
            d[k] = self._validate_margin(d[k])

        try:
            d["Multiplier"] = float(d["Multiplier"])  # there are 0.x values
        except Exception as e:
            print("================")
            print("MULTIPLIER: ", e)
            pprint.pprint(d)
            # exit()

        try:
            d["Closing Price"] = float(d["Closing Price"])
        except Exception as e:
            print("================")
            print("CLOSING PRICE: ", e)
            pprint.pprint(d)
            # exit()
        # pprint.pprint(d)

        return d

    def _scrape_inner_table(self, table):
        """
        Only run on trading Hours etc.
        """
        data = {}
        for tr in table.select("tr"):
            tds = tr.select("td")
            try:
                data[tds[0].text] = tds[1].text
            except:
                continue
        return data

    def _scrape_table(self, data, table):
        """
        Recursively scrape table.
        """
        # print("===========================================")
        # print(table)
        # print("===========================================")
        try:
            table_title = table.select(".underlying")[0].select("th")[0].text.strip()
        except:
            table_title = table.select(".underlying")[0].select("center")[0].text.strip()
        data[table_title] = {}

        for tr in table.children:
            if isinstance(tr, Tag):
                # print(tr)
                # sometimes the row is 2 different structures:
                # th->title, td->value
                # td->title, td->value
                if "underlying" in tr["class"]:
                    continue  # its the table title

                tds = tr.select("td")

                try:
                    row_title = tr.select("th")[0].text.split("\n")[0]
                except:
                    row_title = tds[0].get_text().strip()
                    del tds[0]

                inner_tables = tr.select("table")
                if len(inner_tables) == 1:
                    data[table_title][row_title] = self._scrape_inner_table(inner_tables[0])
                else:
                    text = tds[0].text.strip()
                    data[table_title][row_title] = text
        return data


if __name__ == "__main__":
    ImportInstrumentJson().import_all()
    # def universe_instruments(self, write: bool = True):
    #     from pyfutures.instruments.universe.provider import UniverseProvider

    #     for instrument_id in UniverseProvider.instrument_ids_with_exchange():
    #         exchange = instrument_id.venue
    #         symbol = instrument_id.symbol
    #         path = PACKAGE_ROOT / f"data/ib/instruments/info_html/{exchange}-{symbol}.html"
    #         if not path.exists():
    #             print(f"File does not exist - {path}")
    #             continue
    #         try:
    #             data = self.parse_html_file(path=path)
    #         except ScraperDownloadFailedError as e:
    #             print(e)
    #             pass
    #         outpath = PACKAGE_ROOT / f"data/ib/instruments/info_json/{exchange}-{symbol}.json"
    #         if write:
    #             with open(outpath, "w") as f:
    #                 json.dump(data, f, indent=4)

    # if i < 1:
    #     continue

    # if tr.name == "th":
    # if len(tr.select("th")) == 1 and len(tr.select("td") == 0):
    # its a title

    # tr_children = list(tr.children)
    # row_title = next(tr.children).text
    # 1st child of tr is the row title
    # row_title = tr_children[0].text
    # 2nd is the value and can either be text only or another table
    #
    # match tname:
    #     case "Underlying Information" | "Contract Information" | "Stock Features" | "Margin Requirements":
    #         tdata = self.scrape_table(table)
    #         data[tname] = tdata
    #     case "Contract Identifiers":
    #         tdata = self.scrape_table(table)
    #         rep_tdata = {}
    #         for key in ["ASSETID", "CONID", "ISIN"]:
    #             for tkey, tvalue in tdata.items():
    #                 if tkey.startswith(key):
    #                     rep_tdata[key] = tvalue
    #         print(tdata)
    #         data[tname] = rep_tdata
    #
    #     case _:
    #         # Trading Hours / Price & Size parameters
    #         tdata = self.scrape_table(table)
    #
    #         if "Price Paramaters" in tdata:
    #             price_params = re.sub(r"\n+", "\n", tdata["Price Parameters"]).split("\n")
    #             print("PRICE PARAMS")
    #             print(price_params)
    #
    #         data[tname] = tdata
    #


# def scrape_table(self, table):
#     """bs4 table -> list[dict]"""
#     data = {}
#     data = self._scrape_table(table)
#     # return {tr.select("th")[0].text: tr.select("td")[0].text for tr in table.select("tr")}
#
#     # print(f"================= {tname} ======================")
#     # if re.match(r"^.*\(.*\) Top$", ):
#     # exchange table has Price Parameters, Size Parameters, Trading Hours as inner tables
#     # inner_tables = table.select("table")
#     # print(inner_tables)
#     # for inner_table in inner_tables:
#     # print(data)
#     # is exchange info table
#
#     # if the element contains another table, parse this instead
#
#     # else:
#     # pass
#     # data = self._scrape_table(table)
#     # identify if the table name ends with (.*)$
#     # this means its exchange info and contains trading hours, price params
#     return data

#
# first_child = next(tr.children)
# second_child = next(tr.children)
# print("first: ", first_child, "second: ", second_child)

# data = {}
# column_names = [head.text for head in table.select("th")]
# del column_names[0]
# values = [row.text for row in table.select("td")]
# for i, col_name in enumerate(column_names):
#     try:
#         data[col_name] = int(remove_all_occurrences(values[i], ","))
#     except:
#         # some tickers have margin as "Default"
#         data[col_name] = values[i]

# # Iterate over the rows
# for row in rows:
#     # Create an empty dictionary
#     row_data = {}
#
#     # Iterate over the cells of the row
#     cells = row.find_all("td")
#
#     # Add the text content of each cell to the dictionary
#     for i, cell in enumerate(cells):
#         row_data[column_names[i]] = cell.text
#
#     # Add the row dictionary to the list of rows
#     rows_as_dicts.append(row_data)
#
# return [i for i in rows_as_dicts if i]  # filter empty dicts from list[dict]
# return data


# for i, table in enumerate(tables):
#     print("========================================")
#     print(f"===== TABLE {i} ====")
#     print(table)
#

# ALWAYS use nordVPN proxies
# so I can manually check the instrument pages without a proxy without having to enter captcha


# current_proxy = {} if USE_PROXY else None


# print(soup)
# print(url)
# print("====== we hit a captcha =====")
# print("url copied to clipboard...")
# input("Press Enter to Continue processing after manually entered captcha...")


# def parse_margin_table(self, table):
#     """bs4 table -> list[dict]"""
#     # Iterate over the rows of the table
#     rows = table.find_all("tr")
#
#     data = {}
#     column_names = [head.text for head in table.select("th")]
#     del column_names[0]
#     values = [row.text for row in table.select("td")]
#     print(values)
#     for i, col_name in enumerate(column_names):
#         try:
#             data[col_name] = int(remove_all_occurrences(values[i], ","))
#         except:
#             # some tickers have margin as "Default"
#             data[col_name] = values[i]
#
#     print(data)
#
#     # # Iterate over the rows
#     # for row in rows:
#     #     # Create an empty dictionary
#     #     row_data = {}
#     #
#     #     # Iterate over the cells of the row
#     #     cells = row.find_all("td")
#     #
#     #     # Add the text content of each cell to the dictionary
#     #     for i, cell in enumerate(cells):
#     #         row_data[column_names[i]] = cell.text
#     #
#     #     # Add the row dictionary to the list of rows
#     #     rows_as_dicts.append(row_data)
#     #
#     # return [i for i in rows_as_dicts if i]  # filter empty dicts from list[dict]
#     return data
