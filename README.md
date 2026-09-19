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
- 🔄 **Nested tables** joined by declared `links:` — real `uint64` keys, not
  positional counters that mis-align when containers repeat
- 📐 **Declared row boundaries** via `row:`, so a table's row count is stated in
  the config rather than inferred from which fields happen to be configured
- 🎛️ **Per-field value handling** — `trim`, `on_missing`, `on_invalid`,
  `on_repeat` and `null_values` decide what happens to whitespace and to
  missing, invalid and repeated values
- 🔎 **Config lints** via `parser.warnings()` — advisory warnings about configs
  that are valid but behave surprisingly
- 🎯 **Type conversion** including automatic scale and offset transforms for float fields
- 💡 **Attribute and element extraction** using `@`-prefixed path segments for attributes
- ⏹️ **Early termination** via `stop_at_paths` for efficiently reading only part of a file
- 🐍 **Flexible input** — accepts file paths, path-like objects, readable file-like
  objects, or in-memory `bytes`/`bytearray` (parsed zero-copy, no intermediate buffering)
- 🧵 **Thread-friendly** — the GIL is released while parsing, so threads sharing one
  parser instance can parse multiple documents in parallel
- 🌊 **Bounded-memory streaming** for documents larger than RAM — `parse_batches()`
  yields batches incrementally, and `parse_single_table()` returns a native
  `pyarrow.RecordBatchReader` for Parquet/dataset/DuckDB pipelines

## Installation

```bash
pip install xml2arrow
```

## Usage

### 1. Write a configuration file

A YAML config says which elements become tables, which element makes a row, and
where each column's value is:

```yaml
version: 2
tables:
  - name: stations
    scope: /report/stations         # the element the rows live in
    row: station                    # one row per <station>
    row_id: true                    # adds _id, which readings links to
    fields:
      - {name: id,   path: "@id", data_type: Utf8}
      - {name: name, path: name,  data_type: Utf8}

  - name: readings
    scope: /report/stations/station/readings
    row: reading
    links:
      - parent: stations            # adds _stations_id, which joins to stations._id
    fields:
      - {name: time,  path: "@time", data_type: Utf8}
      - {name: value, path: value,   data_type: Float64}
```

- **`scope`** names the element the rows live in. It also bounds what the table
  captures, and resets `index_of:` positions at every occurrence.
- **`row`** names the element that makes one row: `station` for one row per
  `<station>`, or `"."` for one row per `scope` element, such as a header.
- **`path`** is relative to the row element. A leading slash makes it absolute,
  and `@` marks an attribute.
- **`links`** relates a table to the table it sits inside. `parent:` adds a join
  key, `index_of:` adds a position, and `links: []` adds nothing.
- **Values** are trimmed, and a missing value is null in a `nullable` column
  and an error otherwise. `on_missing`, `on_invalid`, `on_repeat`, `null_values`
  and `trim` change that per field.

The [configuration reference](https://github.com/mluttikh/xml2arrow/blob/develop/docs/configuration.md)
documents every key, the supported data types and the parser options. The
configuration is shared with the Rust crate, so the reference lives there.

> **Configuration format version 1 is deprecated.** A config without
> `version: 2`, which includes every config written for 0.19 and earlier, is
> read as version 1, and a config is one version or the other: a version 1
> config that sets a version 2 key such as `row:` is rejected. Version 1 keeps
> working until 1.0, and `parser.warnings()` lists what it needs to change.
> [Configuration format version 1](https://github.com/mluttikh/xml2arrow/blob/develop/docs/configuration-v1.md)
> documents it, and
> [Migrating to configuration format version 2](https://github.com/mluttikh/xml2arrow/blob/develop/docs/migrating-to-version-2.md)
> moves a config across: `parser.to_version_2()` converts it without changing
> its output.

### 2. Parse the XML

```python
import polars as pl
from xml2arrow import XmlToArrowParser

parser = XmlToArrowParser("config.yaml")
record_batches = parser.parse("data.xml")  # also accepts pathlib.Path, bytes,
# bytearray, or any file-like object

# Access a table by name
batch = record_batches["measurements"]  # pyarrow.RecordBatch

# Convert to a pandas DataFrame
df = batch.to_pandas()

# Convert to a Polars DataFrame
df = pl.from_arrow(batch)

# Convert to a PyArrow Table
import pyarrow as pa

table = pa.Table.from_batches([batch])
```

`parse()` returns a `dict[str, pyarrow.RecordBatch]` whose keys are the table
names defined in your config. Because the values are standard PyArrow
`RecordBatch` objects they integrate directly with pandas, Polars, DuckDB,
and any other tool in the Arrow ecosystem.

> **Tip:** Constructing an `XmlToArrowParser` validates the config and compiles
> its path lookup table once, up front. When processing many files with the same
> config, build the parser **once** and reuse it across `parse()` calls — this
> amortizes that fixed setup cost and is noticeably faster than creating a new
> parser per file, especially for many small documents.
>
> ```python
> parser = XmlToArrowParser("config.yaml")  # validate + compile once
> for path in xml_files:
>     record_batches = parser.parse(path)   # reused for every file
>     ...
> ```

### 3. Check a config for surprises

`XmlToArrowParser(...)` rejects configurations that cannot work. `warnings()`
reports the next tier: configurations that are valid but whose behavior
commonly surprises — a table with no columns, say, or, in a version 1 config,
**row boundaries inferred** from several different child elements,
which yield one partially-filled row per child rather than one row per record.

```python
import logging

parser = XmlToArrowParser("config.yaml")
for warning in parser.warnings():
    logging.warning("xml2arrow config: %s", warning)
```

```text
Table 'header' (xml_path /report/header) has 2 distinct configured child
elements (title, created); row boundaries are inferred, so this table produces
2 partially-filled rows per <header> rather than one. Configuration format
version 2 fixes it by declaring the row: `row: "."` for one row per <header>, or
`row: <element>` to name the repeating element
```

Warnings are plain strings, never printed by the package, and purely advisory:
asking for them cannot change how a document parses. The inferred-boundary
warning carries its own fix, the `row:` line version 2 needs, and a version 2
config never reports it.

A version 1 config, one that does not declare `version: 2`, also gets a
**deprecation notice** that lists exactly what `version: 2` would reject
in that config. See
[Migrating to configuration format version 2](https://github.com/mluttikh/xml2arrow/blob/develop/docs/migrating-to-version-2.md).

Building a parser from a version 1 config also raises
`xml2arrow.exceptions.ConfigVersion1Warning`, a `DeprecationWarning`. Python
shows it when the parser is built in a script's main module, in a notebook or
under pytest, and hides it by default elsewhere. To silence it until you
migrate:

```python
import warnings

from xml2arrow.exceptions import ConfigVersion1Warning

warnings.filterwarnings("ignore", category=ConfigVersion1Warning)
```

`to_version_2()` converts the configuration for you, without changing what it
produces, and lists the parts it leaves for you to decide:

```python
from pathlib import Path

conversion = XmlToArrowParser("config.yaml").to_version_2()
for part in conversion.unconverted:
    print("left for you:", part)
Path("config-v2.yaml").write_text(conversion.yaml)
```

The converted configuration always declares `version: 2`, and loads once
nothing is left for you. The YAML is written fresh, without the original's
comments.

It is worth running once against a real configuration before trusting its row
counts — it is the cheapest signal available, and needs no document.

For the runtime counterpart — "this field matched nothing in *this document*" —
set `parser_options.error_on_unmatched_fields`, which reports every offending
field at once.

### 4. Streaming documents too large for memory

`parse()` materializes every table in full, so peak memory grows with the
document. For XML files that don't fit in memory (multi-GB exports,
Wikipedia-style dumps), `parse_batches()` yields each table's rows
incrementally as `(table_name, batch)` tuples — memory stays bounded by the
batch limits, and parsing runs as you iterate, releasing the GIL so it overlaps
with your processing:

```python
parser = XmlToArrowParser("config.yaml")

for name, batch in parser.parse_batches("huge.xml"):
    writers[name].write_batch(batch)  # e.g. per-table ParquetWriter
```

Concatenating a table's batches in yield order reproduces exactly what
`parse()` would have returned. Batches flush at 8192 rows or 128 MiB of
accumulated values per table (tune with `max_rows_per_batch` /
`max_bytes_per_batch`), and `parser.schema(name)` provides any table's
schema up front for schema-first sinks. One caveat inherent to single-pass
XML: a parent element closes *after* its children, so a child batch can
reference a parent row that arrives in a later batch of the parent table —
irrelevant when each table goes to its own sink.

When the config defines exactly one output table — the common shape for
huge documents — `parse_single_table()` returns a native
`pyarrow.RecordBatchReader`, pluggable directly into
`pyarrow.parquet.ParquetWriter`, `pyarrow.dataset`, or DuckDB:

```python
import pyarrow.parquet as pq

reader = parser.parse_single_table("huge.xml")
with pq.ParquetWriter("out.parquet", reader.schema) as writer:
    for batch in reader:
        writer.write_batch(batch)
```

### 5. Parsers are picklable

A parser can cross a process boundary, so it works directly with
`multiprocessing` and `concurrent.futures.ProcessPoolExecutor`:

```python
from concurrent.futures import ProcessPoolExecutor

parser = XmlToArrowParser("stations.yaml")
with ProcessPoolExecutor() as pool:
    for tables in pool.map(parser.parse, ["a.xml", "b.xml", "c.xml"]):
        ...
```

What crosses is the *configuration*, not the compiled parser: a path-built
parser re-reads its file in the worker, and one built with `from_yaml_str`
carries the YAML inside the pickle. Each worker therefore compiles the config
once, and a path-built parser needs that file to exist on the worker's
filesystem.


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
version: 2
tables:
  - name: report
    scope: /
    row: report
    fields:
      - {name: title,         path: header/title,         data_type: Utf8}
      - {name: created_by,    path: header/created_by,    data_type: Utf8}
      - {name: creation_time, path: header/creation_time, data_type: Utf8}

  - name: stations
    scope: /report/monitoring_stations
    row: monitoring_station
    row_id: true
    links: []                   # inside report's row, but needs no link to it
    fields:
      - {name: id,           path: "@id",                    data_type: Utf8}
      - {name: latitude,     path: location/latitude,        data_type: Float32}
      - {name: longitude,    path: location/longitude,       data_type: Float32}
      - {name: elevation,    path: location/elevation,       data_type: Float32}
      - {name: description,  path: metadata/description,     data_type: Utf8}
      - {name: install_date, path: metadata/install_date,    data_type: Utf8}

  - name: measurements
    scope: /report/monitoring_stations/monitoring_station/measurements
    row: measurement
    links:
      - parent: stations
    fields:
      - {name: timestamp,   path: timestamp,   data_type: Utf8}
      - {name: temperature, path: temperature, data_type: Float64, offset: 273.15}
      - {name: pressure,    path: pressure,    data_type: Float64, scale: 100.0}
      - {name: humidity,    path: humidity,    data_type: Float64}
```

### Parsing and using the output

```python
import polars as pl
from xml2arrow import XmlToArrowParser

parser = XmlToArrowParser("stations.yaml")
# Or XmlToArrowParser.from_yaml_str(yaml) when the config is already in
# memory — an embedded default, one fetched from a service, or one built by a
# tool. It is validated exactly as a file-loaded config is.
record_batches = parser.parse("stations.xml")

stations_df = pl.from_arrow(record_batches["stations"])
measurements_df = pl.from_arrow(record_batches["measurements"])

# Join measurements back to their parent station on the declared key. The
# config's `links: - parent: stations` produced `_stations_id` here, and the
# stations table's `row_id: true` produced its `_id`; the values are global row
# ordinals, so this join stays correct however the document nests or repeats
# its containers.
merged = measurements_df.join(
    stations_df.select(["_id", "id"]),
    left_on="_stations_id",
    right_on="_id",
)
print(merged.select(["id", "timestamp", "temperature", "pressure"]))
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
 ┌─────┬───────┬────────────┬────────────┬────────────┬─────────────────────────────────┬──────────────┐
 │ _id ┆ id    ┆ latitude   ┆ longitude  ┆ elevation  ┆ description                     ┆ install_date │
 │ --- ┆ ---   ┆ ---        ┆ ---        ┆ ---        ┆ ---                             ┆ ---          │
 │ u64 ┆ str   ┆ f32        ┆ f32        ┆ f32        ┆ str                             ┆ str          │
 ╞═════╪═══════╪════════════╪════════════╪════════════╪═════════════════════════════════╪══════════════╡
 │ 0   ┆ MS001 ┆ -61.391106 ┆ 48.086628  ┆ 547.105103 ┆ Located in the Arctic Tundra a… ┆ 2024-03-31   │
 │ 1   ┆ MS002 ┆ 11.891497  ┆ 135.093369 ┆ 174.533493 ┆ Located in the Desert area, us… ┆ 2024-01-17   │
 └─────┴───────┴────────────┴────────────┴────────────┴─────────────────────────────────┴──────────────┘

- measurements:
 ┌──────────────┬──────────────────────┬─────────────┬───────────────┬───────────┐
 │ _stations_id ┆ timestamp            ┆ temperature ┆ pressure      ┆ humidity  │
 │ ---          ┆ ---                  ┆ ---         ┆ ---           ┆ ---       │
 │ u64          ┆ str                  ┆ f64         ┆ f64           ┆ f64       │
 ╞══════════════╪══════════════════════╪═════════════╪═══════════════╪═══════════╡
 │ 0            ┆ 2024-12-30T12:39:15Z ┆ 308.636545  ┆ 95043.997349  ┆ 49.777166 │
 │ 0            ┆ 2024-12-30T12:44:15Z ┆ 302.245167  ┆ 104932.150155 ┆ 32.568715 │
 │ 1            ┆ 2024-12-30T12:39:15Z ┆ 297.941843  ┆ 98940.542872  ┆ 57.707949 │
 │ 1            ┆ 2024-12-30T12:44:15Z ┆ 288.303691  ┆ 100141.305292 ┆ 45.450946 │
 │ 1            ┆ 2024-12-30T12:49:15Z ┆ 269.127444  ┆ 100052.257518 ┆ 70.401175 │
 │ 1            ┆ 2024-12-30T12:54:15Z ┆ 299.002921  ┆ 95376.27857   ┆ 42.620882 │
 └──────────────┴──────────────────────┴─────────────┴───────────────┴───────────┘
```

The `_stations_id` column in the `measurements` table is each measurement's
parent station, added by its `links: - parent: stations` line, and `_id` on
`stations` is the key it refers to, added by that table's `row_id: true`. The values
are global row ordinals rather than per-scope counters, so the join above stays
correct however often `<monitoring_stations>` repeats.
