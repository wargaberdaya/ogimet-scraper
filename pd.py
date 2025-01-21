import pandas as pd


def read_parquet_file():
    # Read the parquet file
    try:
        file_name = "output/station_dump/2025-01-21.parquet"
        # file_name = "output/dump/2025-01-13.parquet"
        #
        df = pd.read_parquet(file_name)
        # Display basic information about the dataframe
        print("\nDataframe Info:")
        print(df.info())

        # Display first few rows
        print("\nFirst few rows:")
        print(df.head(200))

        return df

    except FileNotFoundError:
        print(f"Error: File '{file_name}' not found")
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")


if __name__ == "__main__":
    df = read_parquet_file()
