"""
This script takes the raw JSON output of adapters/apis/coindix.py and converts to
a flat CSV.

Example Usage:
$ python3 process_coindix_file.py \
    coindix_vault_history_20220601.json \
    --output-path=coindix_processed.csv

"""

import json
import pandas as pd
import typer
from typing import Optional

app = typer.Typer()


@app.command()
def convert_cream_file(
    input_path: str, output_path: Optional[str] = None
) -> pd.DataFrame:

    print(f"Loading file: {input_path} ...")
    with open(input_path, "r") as f:
        data = json.load(f)

    # Extract records and convert to dataframe
    print("Extracting records ...")
    records = [o for dd in data for o in dd]
    df = pd.DataFrame(records)

    # Reformat
    print("Reformatting records ...")
    df["borrow_apy"] = (
        df["borrow_apy"].astype(float) / 100.0
    )  # Cast to float and map to unit 1.0
    df["supply_apy"] = (
        df["supply_apy"].astype(float) / 100.0
    )  # Cast to float and map to unit 1.0
    df["date"] = pd.to_datetime(df["date"], utc=True)
    print(f"Finished processing {len(df)} records.")

    if output_path:
        print(f"Saving to CSV: {output_path}")
        df.to_csv(output_path, index=None)

    return df


if __name__ == "__main__":
    app()
