# Shopping Sessions — team Biomakers

DuckDB pipeline for the Databricks Shopping Sessions track (Builders Buildathon, Nov 2026).

- Lines are read raw and checked against the exact CSV dialect with one regular expression per line.
- Timestamps (F1/F2/F3) are validated strictly, then converted with DuckDB's native parser.
- Linked IDs are joined with a union-find; each person is named by its byte-wise smallest ID.
- Events are deduplicated (smallest `ingest_ts`), validated, sessionized with window functions,
  credited to the latest click within 7 days (ASOF join), and assists are counted with a range join.
