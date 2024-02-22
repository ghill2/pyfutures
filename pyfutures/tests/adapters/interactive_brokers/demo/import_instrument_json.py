# import requests
import json
import pprint
from pathlib import Path

from bs4 import BeautifulSoup
from bs4 import Tag


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
            print(path)
            exit()
            with open(path) as f:
                data = self.parse_data(html_data=f.read())

            if write:
                outpath = self._out_folder / f"{path.stem}.json"
                with open(outpath, "w") as f:
                    json.dump(data, f, indent=4)

    def parse_data(self, html_data):
        soup = BeautifulSoup(html_data, "html.parser")
        tables = soup.select("#contractSpecs > table")  # only get first child tables

        data = {}
        if len(tables) > 1:
            for table in tables:
                print(table)

                self._scrape_table(data, table)
        else:
            print(f"Invalid Scraper HTML {html_data}")
            exit()
        exit()
        # redownload
        if data["Underlying Information"]["Description/Name"] == "(@)":
            raise ValueError(f"Failed Instrument: {data}")

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
        try:
            d["Closing Price"] = float(d["Closing Price"])
        except Exception as e:
            print("================")
            print("CLOSING PRICE: ", e)
            pprint.pprint(d)
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
        try:
            table_title = table.select(".underlying")[0].select("th")[0].text.strip()
        except:
            table_title = table.select(".underlying")[0].select("center")[0].text.strip()
        data[table_title] = {}

        for tr in table.children:
            if isinstance(tr, Tag):
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
