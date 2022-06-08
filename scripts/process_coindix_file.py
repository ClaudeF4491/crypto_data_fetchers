"""
This script takes the raw JSON output of adapters/apis/coindix.py and converts to
a flat CSV.

Example Usage:
$ python3 process_coindix_file.py \
    coindix_vault_history_20220601.json \
    --output-path=coindix_vault_history_20220601.csv

"""

import pandas as pd
from tqdm import tqdm
import typer
from typing import Optional

app = typer.Typer()


@app.command()
def convert_coindix_file(
    input_path: str, output_path: Optional[str] = None
) -> pd.DataFrame:

    print(f"Loading file {input_path} ...")
    df = pd.read_json(input_path)
    print(f"Loaded {len(df)} vaults.")

    # Loop through each vault, extract the time series, and append to list
    print("Extracting time-series for each vault ...")
    results = list()
    for i, row in tqdm(df.iterrows()):
        dft = pd.DataFrame(row["series"])
        # Transform the columns
        for c in dft.columns:
            if c == "date":
                dft[c] = pd.to_datetime(dft[c], utc=True)
            else:
                dft[c] = dft[c].astype(
                    float
                )  # Cast to float. Comes in mapped such that 0.1 is 10%

        # Add common fields
        dft["name"] = row["name"]
        dft["protocol"] = row["protocol"]
        dft["chain"] = row["chain"]

        # Append to end
        results.append(dft)
    print("Done!")

    print("Concatenating results and restructuring output ...")
    df_out = pd.concat(results)
    df_out = df_out[
        ["date", "chain", "protocol", "name", "base", "reward", "apy", "tvl"]
    ]

    print(f"Number of records: {len(df_out)}")
    print("Snippet:")
    print(df_out.iloc[100:105, :])

    if output_path:
        print(f"Writing to CSV file: {output_path}")
        df_out.to_csv(output_path, index=None)

    return df_out


if __name__ == "__main__":
    app()
