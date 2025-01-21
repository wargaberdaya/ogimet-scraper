import datetime
import logging
import os
import urllib.request
import warnings
from typing import Optional, Union
from typing import Optional as OptionalType
import time
import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
import requests
import typer
import re

from src.db.sqlite import insert_weather_data, get_weather_data, get_existing_dates

warnings.simplefilter(action="ignore", category=FutureWarning)


def fetch_ogimet_data(
    date: Optional[datetime.datetime] = None,
) -> tuple[datetime.datetime, datetime.datetime, str]:
    """Fetch the HTML content from the OGIMET website."""
    if date is None:
        date = datetime.datetime.now()

    yyyy = date.year
    mm = date.month
    dd = date.day
    hh = 12

    query_date = f"{yyyy}-{mm:02d}-{dd:02d}"
    query_time = f"{hh}:00"

    url = (
        "https://www.ogimet.com/cgi-bin/gsynres"
        f"?lang=en&osum=no&state=Indon&fmt=html"
        f"&ano={yyyy}&mes={mm:02d}&day={dd:02d}"
        f"&hora={hh}&ord=REV"
    )

    # print(f"Fetching data from URL: {url}")

    logging.info(f"Fetching data from URL: {url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cookie": "ogimet_serverid=huracan|Z4N5U|Z4N3p",
        "Cache-Control": "no-cache",
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print("Request timed out - retrying once...")
        time.sleep(2)
        response = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise
    return query_date, query_time, response.text


def fetch_station_data(station_id: str) -> str:
    """Fetch station data from OGIMET website.

    Args:
        station_id: The station ID to fetch data for

    Returns:
        The HTML content from the station data page
    """
    url = f"https://www.ogimet.com/cgi-bin/gsynres?lang=en&ind={station_id}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cookie": "ogimet_serverid=huracan|Z4N5U|Z4N3p",
        "Cache-Control": "no-cache",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print("Request timed out - retrying once...")
        time.sleep(2)
        response = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching station data: {e}")
        raise

    return response.text


class StationData(BaseModel):
    """Model for weather station details."""

    station_id: str = Field(description="Weather station identifier")
    name: str = Field(description="Weather station name")
    latitude: float = Field(description="Station latitude in decimal degrees")
    longitude: float = Field(description="Station longitude in decimal degrees")
    altitude: float = Field(description="Station altitude in meters")


def parse_station_data(html_content: str) -> StationData:
    """Parse the HTML content from the OGIMET website and extract station details.

    Args:
        html_content: Raw HTML content from station data page

    Returns:
        StationData object containing station details
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the station info table
    table = soup.find("table", attrs={"border": "2", "align": "center"})
    if not table:
        raise ValueError("Could not find station info table")

    # Extract station details from table text
    station_text = table.get_text()

    # Parse station ID and name
    id_name = re.search(r"(\d+):.*?([^()]+)", station_text)
    if not id_name:
        raise ValueError("Could not parse station ID and name")
    station_id = id_name.group(1)
    station_name = id_name.group(2).strip()

    # Parse coordinates
    coords = re.search(r"Latitude: (.*?) .*?Longitude: (.*?) ", station_text)
    if not coords:
        raise ValueError("Could not parse coordinates")

    def convert_to_decimal(coord_str: str) -> float:
        # Store original string to check direction later
        orig_coord = coord_str

        # Remove cardinal directions and clean up string
        coord_str = (
            coord_str.replace("N", "")
            .replace("S", "")
            .replace("E", "")
            .replace("W", "")
            .strip()
        )

        # Split into components, handling both space and hyphen separators
        parts = coord_str.replace("-", " ").split()

        # Handle cases with only degrees and minutes
        if len(parts) == 2:
            degrees, minutes = map(float, parts)
            seconds = 0
        else:
            degrees, minutes, seconds = map(float, parts)

        # Calculate decimal degrees
        decimal = degrees + minutes / 60 + seconds / 3600

        # Make negative if South or West
        if "S" in orig_coord or "W" in orig_coord:
            decimal = -decimal

        return decimal

    latitude = convert_to_decimal(coords.group(1))
    longitude = convert_to_decimal(coords.group(2))

    # Parse altitude
    alt = re.search(r"Altitude: (\d+)", station_text)
    if not alt:
        raise ValueError("Could not parse altitude")
    altitude = float(alt.group(1))

    # print(
    #     {
    #         "station_id": station_id,
    #         "name": station_name,
    #         "latitude": latitude,
    #         "longitude": longitude,
    #         "altitude": altitude,
    #     }
    # )

    return StationData(
        station_id=station_id,
        name=station_name,
        latitude=latitude,
        longitude=longitude,
        altitude=altitude,
    )


class WeatherData(BaseModel):
    date: str = Field(
        description="Date of the weather observation in YYYY-MM-DD format"
    )
    time: str = Field(description="Time of the weather observation in HH:MM format")
    station_id: str = Field(description="Weather station identifier")
    station_name: str = Field(description="Weather station name")
    temp_max: OptionalType[float] = Field(None, description="Maximum temperature")
    temp_min: OptionalType[float] = Field(None, description="Minimum temperature")
    temp_med: OptionalType[float] = Field(None, description="Medium temperature")
    wind_dir: OptionalType[str] = Field(None, description="Wind direction")
    wind_speed: OptionalType[float] = Field(None, description="Wind speed")
    wind_gust: OptionalType[float] = Field(None, description="Wind gust speed")
    pressure: OptionalType[float] = Field(None, description="Atmospheric pressure")
    precipitation: OptionalType[float] = Field(None, description="Precipitation amount")
    total_cloud: OptionalType[float] = Field(None, description="Total cloud cover")
    low_cloud: OptionalType[float] = Field(None, description="Low cloud cover")
    sun_duration: OptionalType[float] = Field(None, description="Sun duration")
    visibility: OptionalType[float] = Field(None, description="Visibility distance")
    humidity: OptionalType[float] = Field(None, description="Relative humidity")
    dew_point: OptionalType[float] = Field(None, description="Dew point temperature")
    weather_summary: OptionalType[str] = Field(
        None, description="Weather conditions summary"
    )
    snow_depth: OptionalType[int] = Field(None, description="Snow depth")


def parse_ogimet_data(
    query_date: str,
    query_time: str,
    html_content: str,
) -> list[WeatherData]:
    """Parse the HTML content from the OGIMET website and return list of WeatherData objects."""
    soup = BeautifulSoup(html_content, "html.parser")
    weather_data_batch = []

    # Find the main weather data table
    table = soup.find(
        "table",
        attrs={
            "align": "center",
            "border": "0",
            "cellspacing": "1",
            "bgcolor": "#d0d0d0",
        },
    )

    if table is None:
        print("No weather data table found in HTML content")
        return weather_data_batch

    # Get column positions from headers
    headers = table.find_all("tr")[1:3]  # Get the first two rows (header rows)

    column_map = get_column_mapping(headers)

    if len(column_map) == 0:
        return weather_data_batch

    # example_column_map =  {
    #     "station": 0,
    #     "temp_max": 1,
    #     "humidity": 2,
    #     "wind_dir": 3,
    #     "wind_speed": 4,
    #     "pressure": 5,
    #     "precipitation": 6,
    #     "total_cloud": 7,
    #     "low_cloud": 8,
    #     "sun_duration": 9,
    #     "visibility": 10,
    #     "weather_summary": 11,
    # }

    # Process each row in the table
    for row in table.find_all("tr")[2:]:  # Skip header rows
        cells = row.find_all("td")
        if len(cells) < len(column_map):
            continue

        # Get station info from first column
        station_cell = cells[0].find("a")
        if station_cell and station_cell.get("onmouseover"):
            mouseover = station_cell.get("onmouseover")
            if "CAPTION," in mouseover:
                station = null_if_empty(
                    mouseover.split("CAPTION,")[1].split("'")[1].strip()
                )
            else:
                station = null_if_empty(station_cell.text.strip())
        else:
            station = null_if_empty(cells[0].text.strip())

        if station == "Summary" or not station:
            continue

        try:
            station_id = station.split("-")[0].strip()
            station_name = station.split("-")[1].strip()
        except IndexError:
            print(f"Invalid station format: {station}")
            continue

        # Extract weather data using column positions
        row_data = {
            "date": query_date,
            "time": query_time,
            "station_id": station_id,
            "station_name": station_name,
        }

        # Get temperature values
        if "temp_max" in column_map:
            row_data["temp_max"] = parse_numeric(
                cells[column_map["temp_max"]].text.strip()
            )
        if "temp_min" in column_map:
            row_data["temp_min"] = parse_numeric(
                cells[column_map["temp_min"]].text.strip()
            )
        if "temp_med" in column_map:
            row_data["temp_med"] = parse_numeric(
                cells[column_map["temp_med"]].text.strip()
            )

        # Get humidity and dew point
        if "humidity" in column_map:
            row_data["humidity"] = parse_numeric(
                cells[column_map["humidity"]].text.strip()
            )
        if "dew_point" in column_map:
            row_data["dew_point"] = parse_numeric(
                cells[column_map["dew_point"]].text.strip()
            )

        # Get wind values
        if "wind_dir" in column_map:
            row_data["wind_dir"] = null_if_empty(
                cells[column_map["wind_dir"]].text.strip()
            )
        if "wind_speed" in column_map:
            row_data["wind_speed"] = parse_numeric(
                cells[column_map["wind_speed"]].text.strip()
            )
        if "wind_gust" in column_map:
            row_data["wind_gust"] = parse_numeric(
                cells[column_map["wind_gust"]].text.strip()
            )

        # Get other measurements
        if "pressure" in column_map:
            row_data["pressure"] = parse_numeric(
                cells[column_map["pressure"]].text.strip()
            )
        if "precipitation" in column_map:
            row_data["precipitation"] = parse_numeric(
                cells[column_map["precipitation"]].text.strip()
            )
        if "total_cloud" in column_map:
            row_data["total_cloud"] = parse_numeric(
                cells[column_map["total_cloud"]].text.strip()
            )
        if "low_cloud" in column_map:
            row_data["low_cloud"] = parse_numeric(
                cells[column_map["low_cloud"]].text.strip()
            )
        if "sun_duration" in column_map:
            row_data["sun_duration"] = parse_numeric(
                cells[column_map["sun_duration"]].text.strip()
            )
        if "visibility" in column_map:
            row_data["visibility"] = parse_numeric(
                cells[column_map["visibility"]].text.strip()
            )

        if "snow_depth" in column_map:
            row_data["snow_depth"] = parse_numeric(
                cells[column_map["snow_depth"]].text.strip()
            )

        len_row_data = len(row_data) - 2  # Remove date and time
        len_column_map = len(column_map)

        if len_row_data != len(column_map):
            print(
                f"Row data length ({len_row_data}) does not match column map length ({len_column_map}). "
                f"Column names: {list(column_map.keys())}, Date: {row_data.get('date', 'unknown')}"
            )
            break

        try:
            # print(row_data)
            # print("\n\n")
            weather_data = WeatherData(**row_data)
            weather_data_batch.append(weather_data)
        except Exception as e:
            print(f"Error creating weather data for station {station_id}: {e}")
            continue

    return weather_data_batch


def parse_numeric(value: str) -> Optional[float]:
    """Convert string to number, return None if invalid."""
    value = null_if_empty(value)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def get_column_mapping(header_rows):
    """
    Analyze header rows to determine the column structure and return a mapping of fields to indices.
    """
    headers = []
    for row in header_rows:
        headers.append([cell.get_text(strip=True) for cell in row.find_all(["th"])])

    title_row = headers[0]
    subtitle_row = headers[1]

    expected = {
        "Station": None,
        "Temperature(C)": ["Max", "Min", "Med"],
        "Td.Med(C)": None,
        "Hr.Med(%)": None,
        "Wind(km/h)": ["Dir.", "Int.", "Gust"],
        "Pres.s.lev(Hp)": None,
        "Prec.(mm)": None,
        "TotClOct": None,
        "LowClOct": None,
        "SunD-1(h)": None,
        "VisKm": None,
        "SnowDep.(cm)": None,
        "Dailyweather summary": None,
    }

    # Initialize column mapping
    column_map = {}
    current_index = 0

    # Map fields based on the title and subtitle structure
    for title in title_row:
        if title not in expected:
            print(f"Unknown column header found in first row: {title}")
            current_index += 1
            continue

        # Get the expected subtitles for this title
        subtitles = expected[title]

        if subtitles is None:
            # Handle columns without subtitles
            if title == "Station":
                column_map["station"] = current_index
            elif title == "Td.Med(C)":
                column_map["dew_point"] = current_index
            elif title == "Hr.Med(%)":
                column_map["humidity"] = current_index
            elif title == "Pres.s.lev(Hp)":
                column_map["pressure"] = current_index
            elif title == "Prec.(mm)":
                column_map["precipitation"] = current_index
            elif title == "TotClOct":
                column_map["total_cloud"] = current_index
            elif title == "LowClOct":
                column_map["low_cloud"] = current_index
            elif title == "SunD-1(h)":
                column_map["sun_duration"] = current_index
            elif title == "VisKm":
                column_map["visibility"] = current_index
            elif title == "SnowDep.(cm)":
                column_map["snow_depth"] = current_index
            elif title == "Dailyweather summary":
                column_map["weather_summary"] = current_index
            current_index += 1
        else:
            # Handle columns with subtitles
            for subtitle in subtitles:
                if subtitle in subtitle_row:
                    if title == "Temperature(C)":
                        if subtitle == "Max":
                            column_map["temp_max"] = current_index
                        elif subtitle == "Min":
                            column_map["temp_min"] = current_index
                        elif subtitle == "Med":
                            column_map["temp_med"] = current_index
                    elif title == "Wind(km/h)":
                        if subtitle == "Dir.":
                            column_map["wind_dir"] = current_index
                        elif subtitle == "Int.":
                            column_map["wind_speed"] = current_index
                        elif subtitle == "Gust":
                            column_map["wind_gust"] = current_index
                    current_index += 1

    # No data found
    # https://www.ogimet.com/cgi-bin/gsynres?lang=en&osum=no&state=Indon&fmt=html&ano=2000&mes=03&day=23&hora=12&ord=REV
    if len(column_map) < 2:
        typer.secho("No data found in column mapping", fg=typer.colors.YELLOW)
        return {}

    # Validate that column indices are sequential with no gaps
    max_index = max(column_map.values())
    expected_indices = set(range(max_index + 1))
    actual_indices = set(column_map.values())

    if expected_indices != actual_indices:
        missing_indices = expected_indices - actual_indices
        typer.secho(
            f"Invalid column mapping - missing indices: {missing_indices}",
            fg=typer.colors.RED,
        )
        raise

    return column_map


def fetch_and_parse_data(date: Optional[datetime.datetime] = None) -> None:
    """
    Fetch and parse weather data from OGIMET website, then store in database.

    Args:
        date: Optional datetime object. If not provided, current date will be used.
    """

    # print(f"Fetching weather data for {date}")

    # Fetch the data
    query_date, query_time, html_content = fetch_ogimet_data(date)

    # print(f"Fetched data for {query_date} {query_time}")

    # Parse the data
    weather_data_batch = parse_ogimet_data(query_date, query_time, html_content)

    # Insert the data
    if weather_data_batch:
        try:
            insert_weather_data(weather_data_batch)
        except Exception as e:
            typer.secho(f"Error inserting batch weather data: {e}", fg=typer.colors.RED)
    else:
        typer.secho(
            f"No weather data found for {query_date} {query_time}",
            fg=typer.colors.YELLOW,
        )


def null_if_empty(value: str) -> Optional[str]:
    """Convert empty or dash-only strings to None, otherwise return the original value."""
    empty_values = ["", "-----", "----", "---"]
    if value in empty_values:
        return None
    return value


def create_date_range(
    from_date: str, to_date: Optional[str] = None
) -> list[datetime.datetime]:
    """Create a date range from a start date to an end date.

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: Optional end date in YYYY-MM-DD format

    Returns:
        List of datetime objects representing the date range

    Raises:
        ValueError: If dates are invalid or end date is before start date
    """
    try:
        start_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid start date format: {from_date}. Use YYYY-MM-DD")

    if to_date:
        try:
            end_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid end date format: {to_date}. Use YYYY-MM-DD")

        if end_date < start_date:
            raise ValueError("End date cannot be before start date")

        date_range = [
            start_date + datetime.timedelta(days=x)
            for x in range((end_date - start_date).days + 1)
        ]
        print(f"Fetching weather data from {from_date} to {to_date}")
    else:
        date_range = [start_date]
        print(f"Fetching weather data for {from_date}")

    return date_range


def get_missing_dates(from_date: str, to_date: str) -> list[datetime.datetime]:
    date_range = create_date_range(from_date=from_date, to_date=to_date)
    existing_dates = get_existing_dates()
    return [
        date for date in date_range if date.strftime("%Y-%m-%d") not in existing_dates
    ]


def save_output(df: pd.DataFrame):
    df = df.sort_values("date")
    from_date = df.iloc[0]["date"]
    to_date = df.iloc[-1]["date"]

    folder = f"output/{from_date}_{to_date}"
    os.makedirs(folder, exist_ok=True)

    df.to_json(f"{folder}/data.json", orient="records")
    print(f"Data saved to {folder}/data.json")

    df.to_parquet(f"{folder}/data.parquet")
    print(f"Data saved to {folder}/data.parquet")

    # df.to_excel(f"{folder}/data.xlsx", index=False)
    # print("Data saved to output/data.xlsx")
