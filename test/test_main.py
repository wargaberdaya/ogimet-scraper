from typer.testing import CliRunner
import pytest
from main import app
from src.db import init_database

runner = CliRunner()


@pytest.fixture(autouse=True)
def setup_database():
    """Initialize the database before each test."""
    init_database()


def test_summary_from():
    result = runner.invoke(app, ["summary", "--from", "2024-11-13"])
    assert result.exit_code == 0


def test_summary_from_to():
    result = runner.invoke(
        app, ["summary", "--from", "2024-11-13", "--to", "2024-11-15"]
    )
    assert result.exit_code == 0
