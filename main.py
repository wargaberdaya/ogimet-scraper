import typer
from src.utils import (
    fetch_and_parse_data,
    save_output,
    get_missing_dates,
    fetch_station_data,
    parse_station_data,
)
from src.db.sqlite import (
    get_weather_data,
    init_database,
    get_all_weather_data,
    get_station_list,
    insert_station_details,
    get_missing_stations,
    get_all_station_details,
)
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
    # Define column names based on the SQLite schema
    columns = [
        "date",
        "time",
        "station_id",
        "station_name",
        "temp_max",
        "temp_min",
        "temp_med",
        "wind_dir",
        "wind_speed",
        "wind_gust",
        "pressure",
        "precipitation",
        "total_cloud",
        "low_cloud",
        "sun_duration",
        "visibility",
        "humidity",
        "dew_point",
        "weather_summary",
        "snow_depth",
        "_updated_at",
    ]

    # Create DataFrame with column names
    df = pd.DataFrame(get_all_weather_data(), columns=columns)
    print(f"Found {len(df)} rows")

    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs("output/dump", exist_ok=True)
    df.to_parquet(f"output/dump/{today}.parquet")
    print(f"Data dumped to output/dump/{today}.parquet")


@app.command()
def station():
    init_database()

    station_list = get_missing_stations()
    print(f"Found {len(station_list)} stations")
    print("Fetching station data...")

    def fetch_station_details(station_tuple):
        station_id, _ = station_tuple
        try:
            data = fetch_station_data(station_id)
            details = parse_station_data(data)
            # Store the station details in the database
            insert_station_details(
                station_id=details.station_id,
                name=details.name,
                coords=(details.latitude, details.longitude),
                altitude=details.altitude,
            )
            print(f"Fetched and stored data for station {station_id}")
            return details
        except Exception as e:
            print(f"Error fetching data for station {station_id}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        station_details = list(
            filter(None, executor.map(fetch_station_details, station_list))
        )


@app.command()
def station_dump():
    """Dump station details to parquet, json and csv files."""
    print("Dumping station data to multiple formats")

    # Define column names based on the SQLite schema
    columns = ["station_id", "name", "latitude", "longitude", "altitude", "_updated_at"]

    # Create DataFrame with column names
    df = pd.DataFrame(get_all_station_details(), columns=columns)
    print(f"Found {len(df)} stations")

    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs("output/station_dump", exist_ok=True)

    # Save to parquet
    df.to_parquet(f"output/station_dump/{today}.parquet")
    print(f"Station data dumped to output/station_dump/{today}.parquet")

    # Save to JSON
    df.to_json(f"output/station_dump/{today}.json", orient="records", indent=2)
    print(f"Station data dumped to output/station_dump/{today}.json")

    # Save to CSV
    df.to_csv(f"output/station_dump/{today}.csv", index=False)
    print(f"Station data dumped to output/station_dump/{today}.csv")


if __name__ == "__main__":
    app()
