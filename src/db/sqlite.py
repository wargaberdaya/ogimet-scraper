import os
import sqlite3
from contextlib import contextmanager
from typing import Optional, Union
from pydantic import BaseModel


@contextmanager
def get_db_connection():
    """Create a database connection context manager."""
    db_path = os.getenv("DATABASE_PATH", "weather_data.db")
    # Create the directory if it doesn't exist
    os.makedirs(
        os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True
    )
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def create_weather_table():
    """Create the weather data table if it doesn't exist."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
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
                wind_gust REAL,
                pressure REAL,
                precipitation REAL,
                total_cloud REAL,
                low_cloud REAL,
                sun_duration REAL,
                visibility REAL,
                humidity REAL,
                dew_point REAL,
                weather_summary TEXT,
                snow_depth INTEGER,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, time, station_id)
            )
        """)
        conn.commit()
        print("Weather data table created successfully.")


def create_station_table():
    """Create the station details table if it doesn't exist."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS station_details (
                station_id TEXT PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("Station details table created successfully.")


def insert_weather_data(weather_data: Union[BaseModel, list[BaseModel]]):
    """Insert weather data into SQLite database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        # Handle single record or batch
        if not isinstance(weather_data, list):
            weather_data = [weather_data]

        if not weather_data:
            return

        # Get column names from first record
        data_dict = weather_data[0].model_dump()
        columns = list(data_dict.keys())

        # Prepare values for all records
        values = [
            [record.model_dump()[col] for col in columns] for record in weather_data
        ]

        # Create placeholders for the SQL query
        placeholders = ",".join(["?" for _ in columns])

        # Construct the SQL query with ON CONFLICT clause
        sql = f"""
            INSERT OR REPLACE INTO weather_data ({", ".join(columns)}) 
            VALUES ({placeholders})
        """

        # Execute many inserts at once
        cur.executemany(sql, values)
        conn.commit()


def insert_station_details(
    station_id: str, name: str, coords: tuple[float, float], altitude: float
):
    """Insert or update station details in the database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO station_details 
            (station_id, name, latitude, longitude, altitude)
            VALUES (?, ?, ?, ?, ?)
        """,
            (station_id, name, coords[0], coords[1], altitude),
        )
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

    if from_date and to_date is None:
        conditions.append("date = ?")
        params.append(from_date)

    if from_date and to_date:
        conditions.append("date >= ? AND date <= ?")
        params.append(from_date)
        params.append(to_date)

    if station_id:
        conditions.append("station_id = ?")
        params.append(station_id)

    where_clause = " AND ".join(conditions) if conditions else "1"

    with get_db_connection() as conn:
        cur = conn.cursor()
        query = f"SELECT * FROM weather_data WHERE {where_clause} ORDER BY date, time"
        cur.execute(query, params)
        return cur.fetchall()


def get_all_weather_data() -> list[tuple]:
    """Get all weather data from the database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM weather_data")
        return cur.fetchall()


def get_existing_dates() -> list[str]:
    """Get list of distinct dates in the database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT date FROM weather_data")
        return [date[0] for date in cur.fetchall()]


def get_station_list() -> list[tuple[str, str]]:
    """Get list of distinct stations in the database.

    Returns:
        List of tuples containing (station_id, station_name)
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT station_id, station_name FROM weather_data")
        return cur.fetchall()


def get_missing_stations() -> list[tuple[str, str]]:
    """Get list of stations that exist in weather_data but not in stations table.

    Returns:
        List of tuples containing (station_id, station_name) that need to be added
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        # Get stations that are in weather_data but not in station_details table
        cur.execute("""
            SELECT DISTINCT w.station_id, w.station_name 
            FROM weather_data w
            LEFT JOIN station_details s ON w.station_id = s.station_id
            WHERE s.station_id IS NULL
        """)
        return cur.fetchall()


def init_database():
    """Initialize the database and create tables if they don't exist."""
    create_weather_table()
    create_station_table()


def get_all_station_details() -> list[tuple]:
    """Get all station details from the database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM station_details")
        return cur.fetchall()
