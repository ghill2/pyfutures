# from typing import Callable

# import pandas as pd

# from nautilus_trader.common.actor import Actor
# from nautilus_trader.core.datetime import unix_nanos_to_dt
# from pyfutures.continuous.chain import ContractChain
# from nautilus_trader.model.data import Bar
# from nautilus_trader.model.data import BarType
# from pyfutures.continuous.price import MultiplePrice

# class RollSignal:
#     """
#     Listens to MultiplePrice objects and triggers a roll event
#     """
#     def __init__(
#         self,
#         bar_type: BarType,
#         chain: ContractChain,

#     ):

#         super().__init__()

#         self.topic = f"{bar_type}r"

#         self.chain = chain
#         self.bar_type = bar_type


#     def on_start(self) -> None:


#     """
#     current position stored on chain, multiple positions for each data module
#     each roll signal has a position

#     RollSignal updates ChainView to change bar types for positions in chain
#     Each RollSignal should store current position in chain.
#     get current, contract, carry bar_type from signal
#     ContractData -> RollSignal -> ContractChain
#     """


#     # current_bar_type = self.current_bar_type
#     # forward_bar_type = self.forward_bar_type

#     # if bar.bar_type == current_bar_type or bar.bar_type == forward_bar_type:
#     #     pass
#     # else:
#     #     return

#     # current_bar = self.cache.bar(current_bar_type)
#     # forward_bar = self.cache.bar(forward_bar_type)


#     # # next bar arrived before current or vice versa
#     # if current_bar is None or forward_bar is None:
#     #     return

#     # current_timestamp = unix_nanos_to_dt(current_bar.ts_event)
#     # forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)

# # in_roll_window = (current_timestamp >= self.roll_date) and (current_timestamp.day <= self.expiry_date.day)
