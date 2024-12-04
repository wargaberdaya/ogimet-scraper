import datetime
import logging
import urllib.request
from typing import Optional, Union
from typing import Optional as OptionalType

import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
import warnings

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

    with urllib.request.urlopen(url) as response:
        return query_date, query_time, response.read().decode("utf-8")


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
    pressure: OptionalType[float] = Field(None, description="Atmospheric pressure")
    precipitation: OptionalType[Union[float, str]] = Field(
        None, description="Precipitation amount"
    )
    total_cloud: OptionalType[float] = Field(None, description="Total cloud cover")
    low_cloud: OptionalType[float] = Field(None, description="Low cloud cover")
    sun_duration: OptionalType[float] = Field(None, description="Sun duration")
    visibility: OptionalType[float] = Field(None, description="Visibility distance")
    snow_depth: OptionalType[int] = Field(None, description="Snow depth")
    # weather_conditions: OptionalType[list[dict]] = Field(
    #     None, description="Weather conditions description"
    # )


def parse_ogimet_data(
    query_date: str,
    query_time: str,
    html_content: str,
) -> pd.DataFrame:
    """Parse the HTML content from the OGIMET website and return a pandas DataFrame."""
    soup = BeautifulSoup(html_content, "html.parser")

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
        logging.warning("No weather data table found in HTML content")
        return pd.DataFrame()

    # Initialize lists to store data

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
        "pressure",
        "precipitation",
        "total_cloud",
        "low_cloud",
        "sun_duration",
        "visibility",
        "snow_depth",
        # "weather_conditions",
    ]

    data = pd.DataFrame(columns=columns)

    # Process each row in the table
    for row in table.find_all("tr")[1:]:  # Skip header row
        cells = row.find_all("td")
        if len(cells) < 14:  # Ensure row has enough cells
            continue

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

        if station == "Summary":
            continue

        station_id = station.split("-")[0].strip()
        station_name = station.split("-")[1].strip()
        temp_max = null_if_empty(cells[1].text.strip())
        temp_min = null_if_empty(cells[2].text.strip())
        temp_med = null_if_empty(cells[3].text.strip())
        wind_dir = null_if_empty(cells[6].text.strip())
        wind_speed = null_if_empty(cells[7].text.strip())
        pressure = null_if_empty(cells[8].text.strip())
        precipitation = null_if_empty(cells[9].text.strip())
        total_cloud = null_if_empty(cells[10].text.strip())
        low_cloud = null_if_empty(cells[11].text.strip())
        sun_duration = null_if_empty(cells[12].text.strip())
        visibility = null_if_empty(cells[13].text.strip())
        snow_depth = null_if_empty(cells[14].text.strip())

        # Extract weather conditions from image alt text
        weather_cells = row.find_all("td", bgcolor="#4040FF")
        weather_conditions = []
        for cell in weather_cells:
            img = cell.find("img")
            if img:
                alt_text = img.get("alt", "")
                mouseover = img.get("onmouseover", "")

                # Extract condition and time from mouseover text
                condition = ""
                timestamp = ""
                if "overlib('" in mouseover:
                    mouseover_text = mouseover.split("overlib('")[1].split("');")[0]
                    if "." in mouseover_text and "At" in mouseover_text:
                        condition = mouseover_text.split(".")[0].strip()
                        timestamp = (
                            mouseover_text.split("At")[1].split("UTC")[0].strip()
                        )

                weather_conditions.append(
                    {
                        "image": null_if_empty(alt_text),
                        "condition": null_if_empty(condition),
                        "timestamp": null_if_empty(timestamp),
                    }
                )
            else:
                weather_conditions.append(
                    {"image": None, "condition": None, "timestamp": None}
                )

        # print(f"station: {station}")
        # print(f"temp_max: {temp_max}")
        # print(f"temp_min: {temp_min}")
        # print(f"temp_med: {temp_med}")
        # print(f"wind_dir: {wind_dir}")
        # print(f"wind_speed: {wind_speed}")
        # print(f"pressure: {pressure}")
        # print(f"precipitation: {precipitation}")
        # print(f"total_cloud: {total_cloud}")
        # print(f"low_cloud: {low_cloud}")
        # print(f"sun_duration: {sun_duration}")
        # print(f"visibility: {visibility}")
        # print(f"snow_depth: {snow_depth}")
        # print(f"weather_conditions: {weather_conditions}")

        # print("-" * 100)

        row_data = {
            "date": query_date,
            "time": query_time,
            "station_id": station_id,
            "station_name": station_name,
            "temp_max": temp_max,
            "temp_min": temp_min,
            "temp_med": temp_med,
            "wind_dir": wind_dir,
            "wind_speed": wind_speed,
            "pressure": pressure,
            "precipitation": precipitation,
            "total_cloud": total_cloud,
            "low_cloud": low_cloud,
            "sun_duration": sun_duration,
            "visibility": visibility,
            "snow_depth": snow_depth,
            # "weather_conditions": weather_conditions,
        }

        weather_data = WeatherData(**row_data)

        data = pd.concat(
            [data, pd.DataFrame([weather_data.model_dump()])], ignore_index=True
        )

    return data


def fetch_and_parse_data(date: Optional[datetime.datetime] = None) -> pd.DataFrame:
    """
    Fetch and parse weather data from OGIMET website.

    Args:
        date: Optional datetime object. If not provided, current date will be used.

    Returns:
        pandas DataFrame containing the parsed weather data
    """
    # Fetch the data
    query_date, query_time, html_content = fetch_ogimet_data(date)

    # Parse the data
    df = parse_ogimet_data(query_date, query_time, html_content)

    return df


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
