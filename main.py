import datetime
import typer
from src.utils import fetch_and_parse_data
import pandas as pd

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
    year, month, day = from_date.split("-")
    start_date = datetime.datetime(year=int(year), month=int(month), day=int(day))

    if to_date:
        year, month, day = to_date.split("-")
        end_date = datetime.datetime(year=int(year), month=int(month), day=int(day))
        date_range = [
            start_date + datetime.timedelta(days=x)
            for x in range((end_date - start_date).days + 1)
        ]
    else:
        date_range = [start_date]

    dfs = []
    for date in date_range:
        df = fetch_and_parse_data(date=date)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    filename = f"data_{from_date}"
    if to_date:
        filename += f"-_{to_date}"
    combined_df.to_excel(f"{filename}.xlsx", index=False)


if __name__ == "__main__":
    app()
