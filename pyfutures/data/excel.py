from openpyxl import Workbook
import pandas as pd
from pathlib import Path

def save_excel(df: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.freeze_panes = 'A2'

    