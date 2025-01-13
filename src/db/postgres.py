import os
import psycopg2
from contextlib import contextmanager
from typing import Optional, Union
from pydantic import BaseModel

from dotenv import load_dotenv


load_dotenv()


@contextmanager
def get_db_connection():
    """Create a database connection context manager."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    conn = psycopg2.connect(database_url)
    try:
        yield conn
    finally:
        conn.close()


def create_weather_table():
    """Create the weather data table if it doesn't exist."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First check if the table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'weather_data'
                )
            """)
            table_exists = cur.fetchone()[0]

            if not table_exists:
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
                print("Weather data table created successfully.")
            else:
                print("Weather data table already exists.")


def insert_weather_data(weather_data: Union[BaseModel, list[BaseModel]]):
    """Insert weather data into PostgreSQL database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
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
            placeholders = ",".join(["%s"] * len(columns))

            # Construct the SQL query with ON CONFLICT clause
            sql = f"""
                INSERT INTO weather_data ({", ".join(columns)}) 
                VALUES ({placeholders})
                ON CONFLICT (date, time, station_id) 
                DO UPDATE SET 
                    {", ".join(f"{col} = EXCLUDED.{col}" for col in columns)}
            """

            # Execute many inserts at once
            cur.executemany(sql, values)

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
        conditions.append("date = %s")
        params.append(from_date)

    if from_date and to_date:
        conditions.append("date >= %s AND date <= %s")
        params.append(from_date)
        params.append(to_date)

    if station_id:
        conditions.append("station_id = %s")
        params.append(station_id)

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = (
                f"SELECT * FROM weather_data WHERE {where_clause} ORDER BY date, time"
            )
            cur.execute(query, params)
            return cur.fetchall()


def get_all_weather_data() -> list[tuple]:
    """Get all weather data from the database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM weather_data")
            return cur.fetchall()


def get_existing_dates() -> list[str]:
    """Get list of distinct dates in the database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT date FROM weather_data")
            return [date[0] for date in cur.fetchall()]


def init_database():
    """Initialize the database and create tables if they don't exist."""
    create_weather_table()
