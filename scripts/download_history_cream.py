"""
Downloads all historical data available on CREAM Finance and
saves it to a pre-defined file
"""
from adapters.apis.cream import CreamAdapter
import json
import logging
import typer
from typing import Optional, List

app = typer.Typer()


@app.command()
def download(
    output_path: str, comptrollers: Optional[List[str]] = typer.Argument(None)
) -> None:
    """
    Downloads all CREAM Finance data available across all tokens and history.
    It then saves it to a local JSON file.

    Args:
        output_path: Where to save the results to (JSON)
        comptrollers: Which comptrollers (protocols) to fetch. Defaults to all

    Returns:
        None
    """
    logging.info("Fetching data ...")
    results = CreamAdapter.get_all_token_histories(comptrollers=comptrollers)
    logging.info(f"Fetched {len(results)} records.")
    logging.info(f"Saving results to {output_path} ...")
    with open(output_path, "w") as f:
        json.dump(results, f)
    logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
