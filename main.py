import typer
from src.utils import (
    fetch_and_parse_data,
    save_output,
    get_missing_dates,
)
from src.db import get_weather_data, get_db_connection
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    missing_dates = get_missing_dates(from_date=from_date, to_date=to_date)

    get_db_connection()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(fetch_and_parse_data, date) for date in missing_dates
        ]

        with typer.progressbar(
            as_completed(futures), length=len(futures), label="Fetching weather data"
        ) as progress:
            for future in progress:
                future.result()

    weather_data = get_weather_data(from_date=from_date, to_date=to_date)

    df = pd.DataFrame(
        weather_data,
        columns=[
            "date",
            "time",
            "station_id",
            "station_name",
            "temp_max",
            "temp_min",
            "temp_med",
            "wind_dir",
            "wind_speed",
            "pressure",
            "precipitation",
            "total_cloud",
            "low_cloud",
            "sun_duration",
            "visibility",
            "snow_depth",
        ],
    )
    save_output(df=df)


if __name__ == "__main__":
    app()
