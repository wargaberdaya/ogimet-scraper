import typer
from src.utils import fetch_and_parse_data, create_date_range, save_output
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
    date_range = create_date_range(from_date=from_date, to_date=to_date)

    dfs = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_and_parse_data, date) for date in date_range]

        with typer.progressbar(
            as_completed(futures), length=len(futures), label="Fetching weather data"
        ) as progress:
            for future in progress:
                df = future.result()
                dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    save_output(df=combined_df)


if __name__ == "__main__":
    app()
