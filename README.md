[![PyPI version](https://badge.fury.io/py/xml2arrow.svg)](https://badge.fury.io/py/xml2arrow)
[![Downloads](https://pepy.tech/badge/xml2arrow)](https://pepy.tech/project/xml2arrow)
[![Build Status](https://github.com/mluttikh/xml2arrow-python/actions/workflows/CI.yml/badge.svg)](https://github.com/mluttikh/xml2arrow-python/actions/workflows/CI.yml)
[![Rust](https://img.shields.io/badge/rust-xml2arrow-orange.svg?style=flat&logo=Rust)](https://github.com/mluttikh/xml2arrow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Versions](https://img.shields.io/pypi/pyversions/xml2arrow)](https://pypi.org/project/xml2arrow/)

# xml2arrow-python

A Python package for efficiently converting XML files to Apache Arrow tables using
a YAML configuration. Powered by the [xml2arrow](https://github.com/mluttikh/xml2arrow)
Rust crate for high performance.

## Features

- 🚀 **High-performance** XML parsing via the [xml2arrow](https://github.com/mluttikh/xml2arrow) Rust crate
- 📊 **Declarative mapping** from XML structures to Arrow tables using a YAML config file
- 🔄 **Nested structure support** with parent–child index columns linking related tables
- 🎯 **Type conversion** including automatic scale and offset transforms for float fields
- 💡 **Attribute and element extraction** using `@`-prefixed path segments for attributes
- ⏹️ **Early termination** via `stop_at_paths` for efficiently reading only part of a file
- 🐍 **Flexible input** — accepts file paths, path-like objects, or any file-like object

## Installation

```bash
pip install xml2arrow
```

## Usage

### 1. Write a configuration file

The YAML configuration defines which parts of the XML document become tables and
how their fields are typed. The full schema is:

```yaml
parser_options:
  trim_text: <true|false>      # Trim whitespace from text nodes (default: false)
  stop_at_paths: [<xml_path>]  # Stop parsing after these closing tags (optional,
                               # useful for reading only a file header)
tables:
  - name: <table_name>         # Name of the resulting PyArrow RecordBatch
    xml_path: <xml_path>       # Path to the element whose children are rows.
                               # Use "/" to treat the whole document as one row.
    levels: [<level>, ...]     # Parent-link index columns — see "Nested tables"
    fields:
      - name: <field_name>     # Column name
        xml_path: <field_path> # Path to the element or attribute holding the value.
                               # Prefix the last segment with @ for attributes
                               # (e.g. /library/book/@id)
        data_type: <type>      # Arrow data type — see supported types below
        nullable: <true|false> # Whether the field can be null (default: false)
        scale: <number>        # Multiply float values by this factor (optional)
        offset: <number>       # Add this value to float values after scaling (optional)
```

**Supported data types:** `Boolean`, `Int8`, `UInt8`, `Int16`, `UInt16`, `Int32`,
`UInt32`, `Int64`, `UInt64`, `Float32`, `Float64`, `Utf8`

`Boolean` fields accept (case-insensitively): `true`, `false`, `1`, `0`, `yes`,
`no`, `on`, `off`, `t`, `f`, `y`, `n`.

### 2. Nested tables and `levels`

When your XML has a parent–child relationship between tables, `levels` creates the
index columns that link child rows back to their parent rows. Each string in the
list names an element at a nesting boundary above the row element, and generates a
zero-based `uint32` column named `<level>` in the output.

For example, given stations that each have multiple measurements:

```xml
<report>
  <monitoring_stations>
    <monitoring_station>   <!-- boundary → produces <station> index -->
      <measurements>
        <measurement>      <!-- row element for the measurements table -->
          ...
        </measurement>
      </measurements>
    </monitoring_station>
  </monitoring_stations>
</report>
```

```yaml
- name: measurements
  xml_path: /report/monitoring_stations/monitoring_station/measurements
  levels: [station, measurement]
  fields: [...]
```

This produces a `<station>` column (which parent station each measurement belongs
to) and a `<measurement>` column (the per-station row counter), letting you join
the `measurements` table back to the `stations` table on `<station>`.

### 3. Parse the XML

```python
import pyarrow as pa
from xml2arrow import XmlToArrowParser

parser = XmlToArrowParser("config.yaml")
record_batches = parser.parse("data.xml")  # also accepts pathlib.Path or any file-like object

# Access a table by name
batch = record_batches["measurements"]     # pyarrow.RecordBatch

# Convert to a PyArrow Table
table = pa.Table.from_batches([batch])

# Convert to a pandas DataFrame
df = batch.to_pandas()

# Convert to a Polars DataFrame
import polars as pl
df = pl.from_arrow(batch)
```

`parse()` returns a `dict[str, pyarrow.RecordBatch]` whose keys are the table
names defined in your config. Because the values are standard PyArrow
`RecordBatch` objects they integrate directly with pandas, Polars, DuckDB,
and any other tool in the Arrow ecosystem.

## Example

This example extracts meteorological station data from a nested XML document into
three linked Arrow tables.

### XML data (`stations.xml`)

```xml
<report>
  <header>
    <title>Meteorological Station Data</title>
    <created_by>National Weather Service</created_by>
    <creation_time>2024-12-30T13:59:15Z</creation_time>
  </header>
  <monitoring_stations>
    <monitoring_station id="MS001">
      <location>
        <latitude>-61.39110459389277</latitude>
        <longitude>48.08662749089257</longitude>
        <elevation>547.1050788360882</elevation>
      </location>
      <measurements>
        <measurement>
          <timestamp>2024-12-30T12:39:15Z</timestamp>
          <temperature unit="C">35.486545480326114</temperature>
          <pressure unit="hPa">950.439973486407</pressure>
          <humidity unit="%">49.77716576844861</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:44:15Z</timestamp>
          <temperature unit="C">29.095166644493865</temperature>
          <pressure unit="hPa">1049.3215015450517</pressure>
          <humidity unit="%">32.5687148391251</humidity>
        </measurement>
      </measurements>
      <metadata>
        <description>Located in the Arctic Tundra area, used for Scientific Research.</description>
        <install_date>2024-03-31</install_date>
      </metadata>
    </monitoring_station>
    <monitoring_station id="MS002">
      <location>
        <latitude>11.891496388319311</latitude>
        <longitude>135.09336983543022</longitude>
        <elevation>174.53349357280004</elevation>
      </location>
      <measurements>
        <measurement>
          <timestamp>2024-12-30T12:39:15Z</timestamp>
          <temperature unit="C">24.791842953632283</temperature>
          <pressure unit="hPa">989.4054287187706</pressure>
          <humidity unit="%">57.70794884397625</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:44:15Z</timestamp>
          <temperature unit="C">15.153690541845911</temperature>
          <pressure unit="hPa">1001.413052919951</pressure>
          <humidity unit="%">45.45094598045342</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:49:15Z</timestamp>
          <temperature unit="C">-4.022555715139081</temperature>
          <pressure unit="hPa">1000.5225751769922</pressure>
          <humidity unit="%">70.40117458947834</humidity>
        </measurement>
        <measurement>
          <timestamp>2024-12-30T12:54:15Z</timestamp>
          <temperature unit="C">25.852920542644185</temperature>
          <pressure unit="hPa">953.762785698162</pressure>
          <humidity unit="%">42.62088244545566</humidity>
        </measurement>
      </measurements>
      <metadata>
        <description>Located in the Desert area, used for Weather Forecasting.</description>
        <install_date>2024-01-17</install_date>
      </metadata>
    </monitoring_station>
  </monitoring_stations>
</report>
```

### Configuration (`stations.yaml`)

```yaml
tables:
  - name: report
    xml_path: /
    levels: []
    fields:
      - name: title
        xml_path: /report/header/title
        data_type: Utf8
      - name: created_by
        xml_path: /report/header/created_by
        data_type: Utf8
      - name: creation_time
        xml_path: /report/header/creation_time
        data_type: Utf8

  - name: stations
    xml_path: /report/monitoring_stations
    levels:
      - station
    fields:
      - name: id
        xml_path: /report/monitoring_stations/monitoring_station/@id
        data_type: Utf8
      - name: latitude
        xml_path: /report/monitoring_stations/monitoring_station/location/latitude
        data_type: Float32
      - name: longitude
        xml_path: /report/monitoring_stations/monitoring_station/location/longitude
        data_type: Float32
      - name: elevation
        xml_path: /report/monitoring_stations/monitoring_station/location/elevation
        data_type: Float32
      - name: description
        xml_path: /report/monitoring_stations/monitoring_station/metadata/description
        data_type: Utf8
      - name: install_date
        xml_path: /report/monitoring_stations/monitoring_station/metadata/install_date
        data_type: Utf8

  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
    levels:
      - station      # Links each measurement back to its parent station
      - measurement
    fields:
      - name: timestamp
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/timestamp
        data_type: Utf8
      - name: temperature
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/temperature
        data_type: Float64
        offset: 273.15   # Convert °C → K
      - name: pressure
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/pressure
        data_type: Float64
        scale: 100.0     # Convert hPa → Pa
      - name: humidity
        xml_path: /report/monitoring_stations/monitoring_station/measurements/measurement/humidity
        data_type: Float64
```

### Parsing and using the output

```python
from xml2arrow import XmlToArrowParser

parser = XmlToArrowParser("stations.yaml")
record_batches = parser.parse("stations.xml")

stations_df = record_batches["stations"].to_pandas()
measurements_df = record_batches["measurements"].to_pandas()

# Join measurements back to their parent station using the <station> index
merged = measurements_df.merge(
    stations_df[["<station>", "id"]],
    on="<station>",
)
print(merged[["id", "timestamp", "temperature", "pressure"]])
```

### Output

```text
- report:
 ┌─────────────────────────────┬──────────────────────────┬──────────────────────┐
 │ title                       ┆ created_by               ┆ creation_time        │
 │ ---                         ┆ ---                      ┆ ---                  │
 │ str                         ┆ str                      ┆ str                  │
 ╞═════════════════════════════╪══════════════════════════╪══════════════════════╡
 │ Meteorological Station Data ┆ National Weather Service ┆ 2024-12-30T13:59:15Z │
 └─────────────────────────────┴──────────────────────────┴──────────────────────┘

- stations:
 ┌───────────┬───────┬────────────┬────────────┬────────────┬────────────────────────┬──────────────┐
 │ <station> ┆ id    ┆ latitude   ┆ longitude  ┆ elevation  ┆ description            ┆ install_date │
 │ ---       ┆ ---   ┆ ---        ┆ ---        ┆ ---        ┆ ---                    ┆ ---          │
 │ u32       ┆ str   ┆ f32        ┆ f32        ┆ f32        ┆ str                    ┆ str          │
 ╞═══════════╪═══════╪════════════╪════════════╪════════════╪════════════════════════╪══════════════╡
 │ 0         ┆ MS001 ┆ -61.391106 ┆ 48.086628  ┆ 547.105103 ┆ Located in the Arctic  ┆ 2024-03-31   │
 │           ┆       ┆            ┆            ┆            ┆ Tundra a…              ┆              │
 │ 1         ┆ MS002 ┆ 11.891497  ┆ 135.093369 ┆ 174.533493 ┆ Located in the Desert  ┆ 2024-01-17   │
 │           ┆       ┆            ┆            ┆            ┆ area, us…              ┆              │
 └───────────┴───────┴────────────┴────────────┴────────────┴────────────────────────┴──────────────┘

- measurements:
 ┌───────────┬───────────────┬──────────────────────┬─────────────┬───────────────┬───────────┐
 │ <station> ┆ <measurement> ┆ timestamp            ┆ temperature ┆ pressure      ┆ humidity  │
 │ ---       ┆ ---           ┆ ---                  ┆ ---         ┆ ---           ┆ ---       │
 │ u32       ┆ u32           ┆ str                  ┆ f64         ┆ f64           ┆ f64       │
 ╞═══════════╪═══════════════╪══════════════════════╪═════════════╪═══════════════╪═══════════╡
 │ 0         ┆ 0             ┆ 2024-12-30T12:39:15Z ┆ 308.636545  ┆ 95043.997349  ┆ 49.777166 │
 │ 0         ┆ 1             ┆ 2024-12-30T12:44:15Z ┆ 302.245167  ┆ 104932.150155 ┆ 32.568715 │
 │ 1         ┆ 0             ┆ 2024-12-30T12:39:15Z ┆ 297.941843  ┆ 98940.542872  ┆ 57.707949 │
 │ 1         ┆ 1             ┆ 2024-12-30T12:44:15Z ┆ 288.303691  ┆ 100141.305292 ┆ 45.450946 │
 │ 1         ┆ 2             ┆ 2024-12-30T12:49:15Z ┆ 269.127444  ┆ 100052.257518 ┆ 70.401175 │
 │ 1         ┆ 3             ┆ 2024-12-30T12:54:15Z ┆ 299.002921  ┆ 95376.27857   ┆ 42.620882 │
 └───────────┴───────────────┴──────────────────────┴─────────────┴───────────────┴───────────┘
```

The `<station>` index in the `measurements` table links each measurement to its
parent station by row position, enabling a join on `stations.<station> = measurements.<station>`.
