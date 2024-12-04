import datetime
import os
import typer
from src.utils import fetch_and_parse_data, create_date_range
import pandas as pd

app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def summary(
    from_date: str = typer.Option(..., "--from", help="Date in YYYY-MM-DD format"),
    to_date: str = typer.Option(
        None, "--to", help="Optional end date in YYYY-MM-DD format"
    ),
):
    date_range = create_date_range(from_date=from_date, to_date=to_date)

    dfs = []
    with typer.progressbar(date_range, label="Fetching weather data") as progress:
        for date in progress:
            df = fetch_and_parse_data(date=date)
            dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    filename = f"data_{from_date}"
    if to_date:
        filename += f"-_{to_date}"

    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    combined_df.to_excel(f"output/{filename}.xlsx", index=False)
    print(f"Data saved to output/{filename}.xlsx")


if __name__ == "__main__":
    app()
