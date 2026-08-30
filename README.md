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
  `on_repeat`, `null_values`, each opting out of one historical quirk; `version: 2`
  asserts a config is fully migrated and takes the 1.0 defaults early
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

The YAML configuration defines which parts of the XML document become tables and
how their fields are typed. The full schema is:

```yaml
version: 2                     # Optional. Asserts the config is fully migrated —
                               # every table declares `row:` and uses `links:`
                               # rather than `levels:`, every field uses `path:`
                               # — and in exchange takes the defaults 1.0 will
                               # make mandatory. Omitted means today's behavior.
defaults:                      # Optional. Value policies applied to every field
  trim: <true|false>           # that does not set its own; same keys as a
  on_missing: <policy>         # field's, listed under `fields:` below.
  on_invalid: <policy>
  on_repeat: <policy>
  null_values: [<str>]
parser_options:
  trim_text: <true|false>      # Trim whitespace from text nodes (default: false)
  stop_at_paths: [<xml_path>]  # Stop parsing after these closing tags (optional,
                               # useful for reading only a file header)
  strip_namespaces: <bool>     # Strip ns: prefixes before matching (default: true).
                               # Set false to match raw qualified names and skip
                               # the per-name prefix scan (~4-7% faster); paths must
                               # then spell out any prefix. Free for prefix-free XML.
  allow_truncated_input: <bool> # Accept input that ends mid-element (default: false).
                               # By default a truncated document is an error rather
                               # than a silently short result — see "Security &
                               # trust model". Enable only for recovery tooling.
  error_on_unmatched_fields: <bool>  # Fail if a configured field never matched
                               # anything in the document (default: false). Catches
                               # misspelled xml_paths, whose symptom is otherwise a
                               # silently all-null column. Note that stop_at_paths
                               # leaves everything below the stop path unmatched, so
                               # the two options pull against each other.
  max_value_bytes: <number>    # Cap on the bytes a single field value may accumulate
                               # across text/CDATA/entity events (default: unlimited).
tables:
  - name: <table_name>         # Name of the resulting PyArrow RecordBatch
    xml_path: <xml_path>       # Path to the element whose children are rows.
                               # Use "/" to treat the whole document as one row.
    row: <element>             # Which element finalizes a row (optional).
                               # Omitted: inferred — a row ends whenever ANY
                               #   configured direct child of xml_path closes,
                               #   so two configured children yield two
                               #   half-filled rows per container.
                               # ".": one row per xml_path element itself —
                               #   what metadata tables usually mean.
                               # "name" / "a/b": relative to xml_path.
                               # "/a/b/c": absolute (a LEADING SLASH is what
                               #   makes it absolute; "a/b" is still relative).
    levels: [<level>, ...]     # Parent-link index columns — see "Nested tables".
                               # Optional; replaced by `links:` below.
    links:                     # Declared relationships (optional; not with levels)
      - parent: <table>        #   uint64 join key → <table>._id
        name: <column>         #   default: _<table>_id
      - index_of: <path>       #   uint32 positional ordinal (NOT a key).
        name: <column>         #   Must name an enclosing table's ROW element,
                               #   not its xml_path. Default column name:
                               #   <element>_idx. Same value as <level>.
    row_id: <name|false>       # This table's own key column. Defaults to `_id`
                               # exactly when another table links to it.
    fields:
      - name: <field_name>     # Arrow column name
        path: <field_path>     # Where the value lives. Same rule as `row:`:
                               #   "/a/b/c" is absolute (LEADING SLASH),
                               #   "v" / "sensor/@id" is relative to the row
                               #     element, and needs `row:` on the table.
                               # Prefix the last segment with @ for attributes.
        xml_path: <field_path> # The older name for the same thing, absolute
                               # only. Set exactly one of `path`/`xml_path`;
                               # `xml_path` is removed in 1.0.
        data_type: <type>      # Arrow data type — see supported types below
        nullable: <true|false> # Whether the field can be null (default: false)
                               # If false, a missing/empty value is an error —
                               # except Utf8 fields, which yield "" (see below)
        trim: <true|false>     # Strip surrounding whitespace (optional).
                               # Default: numeric/boolean trim, Utf8 does not.
        on_missing: <policy>   # error | null | empty — what an absent or blank
                               # value becomes. Default depends on the type:
                               # nullable -> null, non-nullable Utf8 -> "",
                               # anything else -> error.
        on_invalid: <policy>   # error | null — what an unparseable value becomes
                               # (default: error).
        on_repeat: <policy>    # error | first | last — what a repeated element
                               # does (default: error).
        null_values: [<str>]   # Literals that count as missing ("N/A", "-").
        scale: <number>        # Multiply float values by this factor (optional)
        offset: <number>       # Add this value to float values after scaling (optional)
                               # value = (value * scale) + offset
```

**Declaring rows (`row:`).** Without it, row boundaries are *inferred* from
whichever fields happen to be configured — so adding a field can change a
table's row count, and a metadata table with three fields produces three
one-third-filled rows instead of one. `row:` states the boundary instead:

```yaml
tables:
  - name: header
    xml_path: /report/header
    row: "."                   # one row per <header>, holding all three fields
    fields: [title, created, version]   # (abbreviated)
```

It is opt-in per table, and it changes nothing else: `levels`, absolute field
paths and scoping all behave as before, and a table that omits `row:` keeps the
inferred rule even when a sibling table declares one. `Config::lint()` reports
tables whose boundaries are inferred from more than one child element, with the
suggested `row:` line in the message — see [Check a config for surprises](#4-check-a-config-for-surprises).

**Relative field paths.** Once a table declares `row:`, its fields can be
written relative to that row element, which removes the repetition of spelling
the full path on every column:

```yaml
tables:
  - name: readings
    xml_path: /report/stations/station/readings
    row: reading
    fields:
      - {name: seq,   path: "@seq",         data_type: Int32}
      - {name: value, path: value,          data_type: Int32}
      - {name: unit,  path: sensor/@unit,   data_type: Utf8}
```

`path` and `xml_path` are two spellings of one location and compile to the same
node, so switching an absolute `xml_path:` to `path:` is a key rename with no
output change. Set exactly one of them per field.

**Declared links (`links:`).** `levels` names *labels* and takes its values
positionally; `links:` names the relationship, which turns a mismatch into a
load-time error and adds a real join key. The two are compared, with what
each produces, in [Linking nested tables](#2-linking-nested-tables) below.

**Value policies.** Every policy above is optional and **absent means current
behavior**, including the type-dependent quirks — setting one opts out of a
specific quirk rather than switching engines. A `defaults:` block at the top of
the config applies them to every field that sets none:

```yaml
defaults:
  trim: true
tables:
  - name: items
    xml_path: /report
    row: item
    fields:
      - {name: n, path: n, data_type: Int32, nullable: true, on_invalid: null}
      - {name: v, path: v, data_type: Utf8, nullable: true, on_repeat: last}
```

The most useful is `on_missing`, because the default is genuinely surprising: a
missing non-nullable `Utf8` field yields `""` while a missing non-nullable
number is an error. `on_missing: error` makes a column behave the same whatever
its type.

**`version: 2`.** Every key above is independently optional, so a config can be
half migrated indefinitely and never say so. `version: 2` is how it says so — an
assertion, not a switch. Declaring it requires the config to be fully migrated
(every table declares `row:`, none uses `levels:`, every field uses `path:`, and
a nested table declares its `links:`), and anything left over is rejected at load
with a message naming it.

In exchange the config opts into the two defaults 1.0 will make mandatory, both
of which today depend on a column's Arrow type rather than on intent: `trim` is
on for every type, and a missing non-nullable value is an error whatever the
type. Per-field policies still win, and deleting the line reverts everything.
See `MIGRATION.md` in the [xml2arrow repository](https://github.com/mluttikh/xml2arrow)
for the full migration guide.

**Supported data types:** `Boolean`, `Int8`, `UInt8`, `Int16`, `UInt16`, `Int32`,
`uint32`, `Int64`, `uint64`, `Float32`, `Float64`, `Utf8`

`Boolean` fields accept (case-insensitively): `true`, `false`, `1`, `0`, `yes`,
`no`, `on`, `off`, `t`, `f`, `y`, `n`.

### 2. Linking nested tables

When one table's rows sit inside another's, the child needs a column saying
which parent row it belongs to. There are two mechanisms, and they are not
equivalent.

**`links:` — a real join key (recommended).** Name the relationship, and the
child gets a `uint64` foreign key pointing at the parent's `_id`:

```yaml
  - name: stations
    xml_path: /report/monitoring_stations
    row: monitoring_station
    fields: [...]

  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
    row: measurement
    links:
      - parent: stations        # uint64 join key -> stations._id
    fields: [...]
```

```python
merged = measurements_df.join(
    stations_df, left_on="_stations_id", right_on="_id"
)
```

The values are **global** row ordinals, never reset, so that join is correct
however often container elements repeat and however the output was batched.
The referenced table materializes `_id` automatically; tables nobody
references gain no column.

**`levels:` — positional index columns (legacy).** Each string names an element
at a nesting boundary above the row element and produces a zero-based `uint32`
column named `<level>`:

```yaml
  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
    levels: [station, measurement]
    fields: [...]
```

This still works and is not going away before 1.0, but it takes its values
*positionally* from whatever ancestor tables happen to enclose the table, and
the counter resets with its scope. That is the difference that matters:

> Two stations in separate containers, three measurements between them:
>
> ```xml
> <group><station><id>A</id><ms><m>1</m><m>2</m></ms></station></group>
> <group><station><id>B</id><ms><m>3</m></ms></station></group>
> ```
>
> | measurement | `parent:` key | positional `<station>` |
> |---|---|---|
> | 1 | 0 | 0 |
> | 2 | 0 | 0 |
> | 3 | **1** | **0** |
>
> The positional column resets with its scope, so it reports `0` for *both*
> stations. Joining on it silently attributes B's measurement to A — a
> plausible DataFrame, wrong data.

**Naming the key columns.** Both are derived from the referenced table's
`name` — `_id` on that table, `_<name>_id` on the one linking to it — and both
can be set explicitly:

```yaml
  - name: stations
    row_id: station_id            # this table's own key
  - name: measurements
    links:
      - parent: stations
        name: station_id          # the foreign key
```

Worth doing whenever the derived names are not what you want downstream; the
derived form interpolates the table's `name` exactly as written, so a table
whose name is not a plain identifier produces a column that is not one either.

Collisions are never resolved silently: if two links, or a link and a field,
would produce the same column, the config is rejected at load with a message
naming `name:` as the fix.

If you are adopting `links:` on an existing config and want the numbers to stay
byte-identical, use `index_of:` rather than `parent:` — it is value-identical to
the `<level>` column for the same path. It is an ordinal, not a key, and carries
the same caveat as `levels`.

*A table defined purely to establish hierarchy — one with an empty `fields`
list — acts only as a boundary and is excluded from the output map.*

### 3. Parse the XML

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

### 4. Check a config for surprises

`XmlToArrowParser(...)` rejects configurations that cannot work. `warnings()`
reports the next tier: configurations that are valid but whose behavior
commonly surprises — most importantly, tables whose **row boundaries are
inferred** from several different child elements, which yield one
partially-filled row per child rather than one row per record.

```python
import logging

parser = XmlToArrowParser("stations.yaml")
for warning in parser.warnings():
    logging.warning("xml2arrow config: %s", warning)
```

```text
Table 'header' (xml_path /report/header) has 2 distinct configured child
elements (title, created); row boundaries are inferred, so this table produces
2 partially-filled rows per <header> rather than one. Declare `row:` to fix it:
`row: "."` for one row per <header>, or `row: <element>` to name the repeating
element
```

Warnings are plain strings, never printed by the package, and purely advisory:
asking for them cannot change how a document parses. The inferred-boundary
warning carries its own fix and goes quiet once you apply it.

It is worth running once against a real configuration before trusting its row
counts — it is the cheapest signal available, and needs no document.

For the runtime counterpart — "this field matched nothing in *this document*" —
set `parser_options.error_on_unmatched_fields`, which reports every offending
field at once.

### 5. Streaming documents too large for memory

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

When the config defines exactly one table with fields — the common shape for
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
    row: report
    fields:
      - {name: title,         path: header/title,         data_type: Utf8}
      - {name: created_by,    path: header/created_by,    data_type: Utf8}
      - {name: creation_time, path: header/creation_time, data_type: Utf8}

  - name: stations
    xml_path: /report/monitoring_stations
    row: monitoring_station
    fields:
      - {name: id,           path: "@id",                    data_type: Utf8}
      - {name: latitude,     path: location/latitude,        data_type: Float32}
      - {name: longitude,    path: location/longitude,       data_type: Float32}
      - {name: elevation,    path: location/elevation,       data_type: Float32}
      - {name: description,  path: metadata/description,     data_type: Utf8}
      - {name: install_date, path: metadata/install_date,    data_type: Utf8}

  - name: measurements
    xml_path: /report/monitoring_stations/monitoring_station/measurements
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
# config's `links: - parent: stations` produced `_stations_id` here and `_id`
# on the stations table; the values are global row ordinals, so this join
# stays correct however the document nests or repeats its containers.
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

The `<station>` index in the `measurements` table links each measurement to its
parent station by row position, enabling a join on `stations.<station> = measurements.<station>`.
