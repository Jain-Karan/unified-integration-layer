# unified-integration-layer

Declarative integration of SAP, Salesforce, Kafka and vendor flat files into one canonical
customer/order model on Apache Iceberg.

Each source is a YAML mapping (`src/uil/mappings/`): a reader, a source contract, and a
field-by-field mapping onto the canonical Pydantic model. Records that break either contract go
to an Iceberg quarantine table with the reason; good rows are upserted with lineage columns.

```bash
pip install -e ".[dev]"
uil generate --out data/feeds          # synthetic SAP / Salesforce / Kafka / vendor feeds
uil ingest --feeds data/feeds --lake data/lake
uil query "select _source, count(*) from customer group by 1"
pytest
```

Work in progress.
