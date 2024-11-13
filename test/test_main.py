from typer.testing import CliRunner

from main import app, fetch

# runner = CliRunner()


def test_app():
    # result = runner.invoke(app, ["fetch"])

    # # assert result.exit_code == 0

    fetch()
