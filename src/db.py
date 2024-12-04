import sqlite3
from contextlib import contextmanager
from typing import Optional
from pydantic import BaseModel


@contextmanager
def get_db_connection():
    """Create a database connection context manager."""
    conn = sqlite3.connect("weather_data.db")
    try:
        yield conn
    finally:
        conn.close()


def create_weather_table():
    """Create the weather data table if it doesn't exist."""
    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                date TEXT,
                time TEXT,
                station_id TEXT,
                station_name TEXT,
                temp_max REAL,
                temp_min REAL,
                temp_med REAL,
                wind_dir TEXT,
                wind_speed REAL,
                pressure REAL,
                precipitation TEXT,
                total_cloud REAL,
                low_cloud REAL,
                sun_duration REAL,
                visibility REAL,
                snow_depth INTEGER,
                PRIMARY KEY (date, time, station_id)
            )
        """)
        conn.commit()


def insert_weather_data(weather_data: BaseModel):
    """Insert weather data into SQLite database."""
    create_weather_table()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        data_dict = weather_data.model_dump()
        placeholders = ", ".join(["?" for _ in data_dict])
        columns = ", ".join(data_dict.keys())
        sql = f"INSERT OR REPLACE INTO weather_data ({columns}) VALUES ({placeholders})"

        cursor.execute(sql, list(data_dict.values()))
        conn.commit()


def get_weather_data(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    station_id: Optional[str] = None,
) -> list[tuple]:
    """
    Retrieve weather data from the database with optional filters.

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        station_id: Station identifier

    Returns:
        List of weather data records
    """
    conditions = []
    params = []

    if from_date:
        conditions.append("date >= ?")
        params.append(from_date)

    if to_date:
        conditions.append("date <= ?")
        params.append(to_date)

    if station_id:
        conditions.append("station_id = ?")
        params.append(station_id)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = f"SELECT * FROM weather_data WHERE {where_clause} ORDER BY date, time"
        cursor.execute(query, params)
        return cursor.fetchall()


def get_existing_dates() -> list[str]:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM weather_data")
        return [date[0] for date in cursor.fetchall()]
