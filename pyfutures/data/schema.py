# from __future__ import annotations
# from copy import copy
# import numpy as np
# import pandas as pd
# from nautilus_trader.model.objects import FIXED_SCALAR
# from nautilus_trader.model.objects import QUANTITY_MAX

# from pytower.core.datetime import dt_to_unix_nanos_vectorized
# from pytower.core.datetime import unix_nanos_to_dt_vectorized


# from nautilus_trader.model.enums import AssetClass
# from nautilus_trader.model.identifiers import InstrumentId


# import pyarrow as pa


# class Schema:
#     # @classmethod
#     # def from_dataframe(cls: type, df: pd.DataFrame):
#     #     """
#     #     constructor function to set the from_schema dict from a given dataframe
#     #     used by ParquetFile.read(), write() to validate data read from file against nautilus/pytower schemas
#     #     """
#     #     return cls({k: v for k, v in zip(df.columns, df.dtypes)})
#     #
#     # @classmethod
#     # def get(cls, instrument_id: InstrumentId, asset_class: AssetClass, cls: type):
#     # from pytower.data.schemas import SCHEMAS
#     # return cls(schema=schema)
#     # """pass in nautilus parameters to get the schema from schemas dict"""
#     @staticmethod
#     def from_dataframe(df: pd.Dataframe):
#         return {k: v for k, v in zip(df.columns, df.dtypes)}

#     @staticmethod
#     def validate(from_schema: dict, to_schema: dict, raise_error: bool = True):
#         """validates this schema against a given schema"""

#         """if matches schema / validation"""
#         """asserts if type match"""
#         # from_schema = {k: v for k, v in zip(df.columns, df.dtypes)}

#         # get columns and assert them equal as 2 lists
#         column_names_match = from_schema.keys() == to_schema.keys()
#         if not column_names_match:
#             err_str = f"""
#             df columns for source schema does not match to_schema:
#             FROM: {from_schema.keys()}
#             TO: {to_schema.keys()}
#             """
#             if raise_error:
#                 raise RuntimeError(f"ERROR: {err_str}")
#             else:
#                 print(f"WARNING: {err_str}")

#         # types = self.schema.values() == to_schema.values()
#         # asserts all columns to the given type defined in the output schema
#         for [col, output_type] in to_schema.items():
#             if from_schema[col] != output_type:
#                 err_str = f"""
#                 df column name {col} with type {from_schema[col]}
#                     - does not match output schema type {output_type}
#                 """
#                 if raise_error:
#                     raise RuntimeError(f"ERROR: {err_str}")
#                 else:
#                     print(f"WARNING: {err_str}")

#     @staticmethod
#     def cast_safely(df: pd.DataFrame, to_schema: dict) -> pd.DataFrame:
#         """
#         processes the input dataframe using the schema
#         all columns not defined in the schema will be dropped from the input df
#         timestamp type is set to DT64NS_DTYPE in input df
#         attempts to cast a dataframe schema to another dataframe schema
#         """

#         # set timestamp to DT64NS_DTYPE
#         df["timestamp"] = df["timestamp"].astype(to_schema["timestamp"])

#         return df

#     # def __repr__(self) -> str:
#     # return str(self.schema)


# #
# # def to_stored_format(df: pd.DataFrame):
# #     # Convert to persistence format
# #     cols = list(df.reset_index().columns)
# #
# #     df.reset_index(inplace=True)
# #     df.rename({"timestamp": "ts_event"}, axis=1, inplace=True)
# #
# #     df["ts_event"] = dt_to_unix_nanos_vectorized(df["ts_event"])
# #
# #     if "ts_init" not in cols:
# #         df["ts_init"] = df["ts_event"]
# #
# #     df["bid"] = (df["bid"] * FIXED_SCALAR).astype(np.int64)
# #     df["ask"] = (df["ask"] * FIXED_SCALAR).astype(np.int64)
# #
# #     if "ask_size" in cols:
# #         df["ask_size"] = (df["ask_size"] * FIXED_SCALAR).astype(np.int64)
# #     else:
# #         df["ask_size"] = QUANTITY_MAX
# #
# #     if "bid_size" in cols:
# #         df["bid_size"] = (df["bid_size"] * FIXED_SCALAR).astype(np.int64)
# #     else:
# #         df["bid_size"] = QUANTITY_MAX
# #
# #     df["ask_size"] = df["ask_size"].astype(np.uint64)
# #     df["bid_size"] = df["bid_size"].astype(np.uint64)
# #
# #     expected_columns = "bid ask ask_size bid_size ts_event ts_init".split()
# #     df = df[expected_columns]
# #
# #     assert_stored_format(df)
# #
# #     return df
# #
# #
# # def to_unstored_format(df: pd.DataFrame):
# #     # cols = list(df.reset_index().columns)
# #     # if 'index' in cols:
# #     #     df.drop('index', axis=1, inplace=True)
# #     df["bid"] = df["bid"].astype(float) / FIXED_SCALAR
# #     df["ask"] = df["ask"].astype(float) / FIXED_SCALAR
# #     df["ask_size"] = df["ask_size"].astype(float) / FIXED_SCALAR
# #     df["bid_size"] = df["bid_size"].astype(float) / FIXED_SCALAR
# #     # df['timestamp'] = df['ts_event'].apply(unix_nanos_to_dt)
# #     df["timestamp"] = unix_nanos_to_dt_vectorized(df["ts_event"])
# #     df.set_index("timestamp", inplace=True)
# #     df.drop(["ts_event", "ts_init"], axis=1, inplace=True)
# #     return df
