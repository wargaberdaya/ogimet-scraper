from typer.testing import CliRunner

from main import app

runner = CliRunner()


def test_summary_from():
    result = runner.invoke(app, ["summary", "--from", "2024-11-13"])
    assert result.exit_code == 0


def test_summary_from_to():
    result = runner.invoke(
        app, ["summary", "--from", "2024-11-13", "--to", "2024-11-15"]
    )
    assert result.exit_code == 0
