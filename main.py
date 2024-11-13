import datetime
import typer
from src.utils import fetch_ogimet_data, parse_ogimet_data

app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def fetch():
    html = fetch_ogimet_data()
    df = parse_ogimet_data(html)
    # save to csv
    # df.to_csv("data.csv", index=False)
    # sace to excel
    df.to_excel("data.xlsx", index=False)


if __name__ == "__main__":
    app()
