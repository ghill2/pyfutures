from nautilus_trader.model.enums import BarAggregation
from io import StringIO
from pathlib import Path
import pandas as pd
import numpy as np

PORTARA_DATA_FOLDER = Path("/Users/g1/Downloads/portara_data_george")

class PortaraData:
    
    @staticmethod
    def get_paths(
        data_symbol: str,
        aggregation: BarAggregation,
    ):
        
        if aggregation == BarAggregation.DAY:
            folder = (PORTARA_DATA_FOLDER / "DAY" / data_symbol)
            paths = list(folder.glob("*.txt")) + list(folder.glob("*.bd"))
        elif aggregation == BarAggregation.MINUTE:
            folder = (PORTARA_DATA_FOLDER / "MINUTE" / data_symbol)
            paths = list(folder.glob("*.txt")) + list(folder.glob("*.b01"))
        
        assert len(paths) > 0
        return list(sorted(paths))
    
    @staticmethod
    def read_dataframe(path: Path) -> pd.DataFrame:
        
        path = Path(path)
        
        try:
            with open(path, 'r', encoding="utf-8") as f:
                column_count = len(f.readline().split(","))
        except UnicodeDecodeError as e:  # corrupted file
            print(path)
            raise e
            
        
        if path.suffix == ".bd" and column_count == 8: # daily .bd file
            dtype = {
                "symbol": str,
                "day": int,
                "open": np.float64,
                "high": np.float64,
                "low": np.float64,
                "close": np.float64,
                "tick_count": np.int64,
                "volume": np.float64,
            }
        elif path.suffix == ".txt" and column_count == 14:  # EBM 4 missing years
            dtype = {
                "day": int,
                "open": np.float64,
                "high": np.float64,
                "low": np.float64,
                "close": np.float64,
                "tick_count": np.int64,
                "volume": np.float64,
                "ignored1": int,
                "ignored2": int,
                "ignored3": int,
                "ignored4": np.float64,
                "ignored5": np.float64,
                "ignored6": np.float64,
                "ignored7": np.float64,
            }
        elif path.suffix in (".txt", ".b01") and column_count == 7:  # daily .txt file
            dtype = {
                "day": int,
                "open": np.float64,
                "high": np.float64,
                "low": np.float64,
                "close": np.float64,
                "tick_count": np.int64,
                "volume": np.float64,
            }
        elif path.suffix in (".txt", ".b01") and column_count == 8:  # minute .txt or .b01 file
            dtype = {
                "day": int,
                "time": int,
                "open": np.float64,
                "high": np.float64,
                "low": np.float64,
                "close": np.float64,
                "tick_count": np.int64,
                "volume": np.float64,
            }
        else:
            raise RuntimeError(str(path))
        
        with open(path, "r", encoding="utf-8") as f:
            body = f.read()
            
        missing = _get_missing_rows(path)
        if missing is not None:
            missing = "\n".join(
                list(map(lambda x: x.strip(), missing.strip().splitlines()))
            )
            body = body + missing
        
        df = pd.read_csv(StringIO(body), names=list(dtype.keys()), dtype=dtype)
        
        if "symbol" in df.columns:
            df.drop(["symbol"], axis=1, inplace=True)
            
        if "time" in df.columns:
            timestamps = pd.to_datetime(
                    (df["day"] * 10000 + df["time"]).astype(str),
                    format="%Y%m%d%H%M",
                    utc=True,
                )
            df.insert(0, 'timestamp', timestamps)
            df.drop(["time"], axis=1, inplace=True)
        else:
            timestamps = pd.to_datetime(df["day"], format="%Y%m%d", utc=True)
            df.insert(0, 'timestamp', timestamps)
            

        df.drop(["day", "tick_count"], axis=1, inplace=True)
        
        df = df[df.columns[:6]]
        
        return df
    
def _get_missing_rows(path: Path) -> str | None:
    if path.parent.parent.stem == "DAY" and path.stem == "ZIN2008F":  # NIFTY
        return """
        20071227,6120.0,6120.0,6120.0,6120.0,5000,100000
        20071228,6119.5,6119.5,6119.5,6119.5,5000,100000
        20071231,6155.0,6155.0,6155.0,6155.0,5000,100000
        20080101,6156.5,6156.5,6156.5,6156.5,5000,100000
        20080102,6223.0,6223.0,6223.0,6223.0,5000,100000
        20080103,6178.0,6178.0,6178.0,6178.0,5000,100000
        20080104,6145.0,6289.0,6145.0,6255.0,4596,106508
        20080107,6139.5,6288.0,6139.5,6288.0,4720,113831
        20080108,6279.0,6320.0,6195.0,6269.0,6614,120547
        20080109,6250.5,6318.0,6200.0,6260.0,2668,129891
        20080110,6265.0,6312.0,6112.5,6162.0,3052,131973
        20080111,6149.0,6235.0,6095.0,6222.0,5011,133297
        20080114,6200.0,6225.0,6160.0,6225.0,2059,139088
        20080115,6250.0,6250.0,6040.0,6058.0,4067,156727
        20080116,6000.0,6030.0,5800.0,5947.0,16582,187309
        20080117,5861.0,6035.0,5810.0,5922.0,5594,181865
        20080118,5860.0,5918.5,5680.0,5720.0,11137,189895
        20080121,5600.0,5615.0,4850.0,5198.0,19482,204858
        20080122,4900.5,5098.0,4419.5,4920.0,21660,196269
        20080123,5000.0,5340.0,4940.0,5164.0,27372,197447
        20080124,5041.0,5370.0,4951.0,5001.0,14343,188951
        20080125,5025.0,5404.0,4951.0,5404.0,9689,180177
        20080128,5240.0,5277.0,5040.0,5251.5,8483,170850
        20080129,5250.0,5360.0,5200.0,5277.0,6827,145856
        20080130,5280.0,5380.0,5100.0,5167.0,6940,92295
        20080131,5150.0,5351.0,5056.0,5138.0,0,0
        """
    elif path.parent.parent.stem == "DAY" and path.stem == "ESU2018Z":  # FESU
        return """
        ESU2018Z,20181217,290.6,290.6,290.6,290.6,20,20
        ESU2018Z,20181218,286.4,286.4,286.4,286.4,20,20
        ESU2018Z,20181219,289.7,289.7,289.7,289.7,20,20
        ESU2018Z,20181220,287.3,287.3,287.3,287.3,20,20
        ESU2018Z,20181221,284.7,284.7,284.7,284.7,20,20
        """
        
    elif path.parent.parent.stem == "DAY" and path.stem == "ESU2019H":  # FESU
        return """
        ESU2019H,20181214,287.5,287.5,287.5,287.5,20,20
        ESU2019H,20181217,287.6,287.6,287.6,287.6,20,20
        ESU2019H,20181218,283.4,283.4,283.4,283.4,20,20
        ESU2019H,20181219,286.7,286.7,286.7,286.7,20,20
        ESU2019H,20181220,284.3,284.3,284.3,284.3,20,20
        ESU2019H,20181221,283.8,283.8,283.8,283.8,20,20
        ESU2019H,20181227,275.2,275.2,275.2,275.2,20,20
        """
    return None
        
        
    