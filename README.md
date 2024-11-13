
# OGIMET Weather Data Scraper

> ⚠️ Still in development. The code may break at any time.

A Python tool to fetch and parse weather data from the OGIMET website for Indonesian weather stations.

## Features

- Fetch weather data for specific dates or date ranges
- Parse weather station information including:
  - Temperature (max, min, medium)
  - Wind direction and speed
  - Atmospheric pressure
  - Precipitation
  - Cloud cover
  - Sun duration
  - Visibility
  - Snow depth
- Export data to Excel format

## Installation

1. Clone this repository

```
git clone https://github.com/wargaberdaya/ogimet-scraper.git
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

The tool provides a command-line interface with the following commands:

### Fetch weather data for a single date

```bash
python main.py summary --from 2024-03-20
```

### Fetch weather data for a date range

```bash
python main.py summary --from 2024-03-01 --to 2024-03-20
```

The data will be exported to an Excel file named `data_YYYY-MM-DD.xlsx` for single dates or `data_YYYY-MM-DD-_YYYY-MM-DD.xlsx` for date ranges.
