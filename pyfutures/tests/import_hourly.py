from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.data.writer import BarParquetWriter
from nautilus_trader.model.data import Bar
from pyfutures.data.files import ParquetFile
from nautilus_trader.model.enums import BarAggregation
import joblib
from nautilus_trader.model.data import BarType
from pyfutures.tests.adapters.interactive_brokers.test_kit import PER_CONTRACT_FOLDER

def process(file: ParquetFile, row: dict) -> None:
    
    df = file.read(
        to_aggregation=(1, BarAggregation.HOUR),
    )
    
    bar_type = BarType.from_str(str(file.bar_type).replace("MINUTE", "HOUR"))
    outfile = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )
    
    writer = BarParquetWriter(
        path=outfile.path,
        bar_type=bar_type,
        price_precision=row.base.price_precision,
        size_precision=1,
    )
    
    print(f"Writing {bar_type} {outfile}...")
    writer.write_dataframe(df)
    
def func_gen():
    
    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    
    for row in rows:
        
        files = IBTestProviderStubs.bar_files(
            row.trading_class, BarAggregation.MINUTE,
        )
        for file in files:
            yield joblib.delayed(process)(
                file=file,
                row=row,
            )
        
if __name__ == "__main__":
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen())