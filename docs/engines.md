# Reading the canonical tables from other engines

The lake is standard Apache Iceberg: Parquet data files plus metadata JSON. Locally the catalog
is SQLite (`<lake>/catalog.db`) and the warehouse is `<lake>/warehouse`. In a real deployment
you'd point PyIceberg at a shared catalog (AWS Glue, a REST catalog such as Polaris, or Unity
Catalog) and an object-store warehouse by changing `Lake.__init__`. Nothing else changes.

> **Tested here:** PyIceberg and DuckDB (`uil query`, every test).
> **Not tested here:** the Snowflake and Databricks steps below. They follow those products'
> documented Iceberg support. Check current syntax against their docs.

## DuckDB

```python
from uil.lake import Lake

Lake("data/lake").query('select status, count(*) from "order" group by 1')
```

or directly from a metadata file with DuckDB's `iceberg` extension:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('data/lake/warehouse/canonical/order/metadata/<latest>.metadata.json');
```

## Snowflake (externally managed Iceberg table)

With the warehouse in S3 and the catalog in Glue:

```sql
CREATE CATALOG INTEGRATION uil_glue
  CATALOG_SOURCE = GLUE  CATALOG_NAMESPACE = 'canonical'  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = '<role-arn>'  GLUE_CATALOG_ID = '<account-id>'  ENABLED = TRUE;

CREATE EXTERNAL VOLUME uil_lake
  STORAGE_LOCATIONS = ((NAME = 'lake' STORAGE_PROVIDER = 'S3'
                        STORAGE_BASE_URL = 's3://<bucket>/warehouse/'
                        STORAGE_AWS_ROLE_ARN = '<role-arn>'));

CREATE ICEBERG TABLE canonical_order
  EXTERNAL_VOLUME = 'uil_lake'  CATALOG = 'uil_glue'  CATALOG_TABLE_NAME = 'order';
```

## Databricks

Either register the Glue/REST catalog as a foreign catalog through Lakehouse Federation, or
read a table's location directly:

```sql
SELECT * FROM iceberg.`s3://<bucket>/warehouse/canonical/order`;
```
