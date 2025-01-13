import typer
from src.utils import (
    fetch_and_parse_data,
    save_output,
    get_missing_dates,
)
from src.db.sqlite import get_weather_data, init_database, get_all_weather_data
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os


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
    save: bool = typer.Option(False, "--save", help="Save output to file"),
):
    init_database()

    missing_dates = get_missing_dates(from_date=from_date, to_date=to_date)

    print(f"Found {len(missing_dates)} missing dates")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(fetch_and_parse_data, date) for date in missing_dates
        ]

        with typer.progressbar(
            as_completed(futures), length=len(futures), label="Fetching weather data"
        ) as progress:
            for future in progress:
                future.result()


@app.command()
def dump():
    print("Dumping data to parquet")
    df = pd.DataFrame(get_all_weather_data())
    print(f"Found {len(df)} rows")

    today = datetime.now().strftime("%Y-%m-%d")

    os.makedirs("output/dump", exist_ok=True)

    df.to_parquet(f"output/dump/{today}.parquet")

    print(f"Data dumped to output/dump/{today}.parquet")


if __name__ == "__main__":
    app()
