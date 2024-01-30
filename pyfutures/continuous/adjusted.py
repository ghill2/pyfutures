import pandas as pd
from pyfutures.continuous.price import MultiplePrice
from collections import deque
from copy import copy
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import BarType

class AdjustedPrices:
    
    def __init__(
        self,
        lookback: int,
        bar_type: BarType,  # bar type that triggers the adjustment
    ):
        self._lookback = lookback
        self._adjusted_prices = {}
        self._multiple_prices = {}
        
        self._bar_type = bar_type
    
    def handle_continuous_price(self, price: MultiplePrice) -> None:
        
        if self._adjusted_prices.get(str(price.bar_type)) is None:
            self._adjusted_prices[str(price.bar_type)] = deque(maxlen=self._lookback)
            
        if self._multiple_prices.get(str(price.bar_type)) is None:
            self._multiple_prices[str(price.bar_type)] = deque(maxlen=self._lookback)
            
        if len(self._multiple_prices[str(price.bar_type)]) > 0:
            
            last = self._multiple_prices[str(price.bar_type)][-1]
            
            if last.current_month != price.current_month:
                
                if last.forward_price is None or last.current_price is None:
                    print(f"No forward or current price found for {price.instrument_id}")
                    exit()
                
                roll_differential = float(last.forward_price) - float(last.current_price)
                self._adjusted_prices[str(price.bar_type)] = deque(
                    pd.Series(self._adjusted_prices[str(price.bar_type)]) + roll_differential,
                    maxlen=self._lookback,
                )
            
        self._adjusted_prices[str(price.bar_type)].append(price.current_price)
        self._multiple_prices[str(price.bar_type)].append(price)
        
        assert len(self._multiple_prices[str(price.bar_type)]) == len(self._adjusted_prices[str(price.bar_type)])
    
    def to_dataframe(self) -> pd.DataFrame:
        dfs = []
        for bar_type, prices in self._multiple_prices.items():
            df = pd.DataFrame(
                list(map(MultiplePrice.to_dict, prices))
            )
            
            adjusted = self._adjusted_prices[str(bar_type)]
            df[f"adjusted_{bar_type.spec}"] = list(map(float, adjusted))
            
            df["timestamp"] = list(map(unix_nanos_to_dt, df["ts_event"]))
            dfs.append(dfs)
            
        df = pd.concat([dfs], axis=1)
        df.sort_values("timestamp", inplace=True)
        return df
    
    
    # def to_series(self) -> pd.Series:
    #     return pd.Series(
    #         self._adjusted_prices,
    #         index=map(unix_nanos_to_dt, self._adjusted_timestamps),
    #     )