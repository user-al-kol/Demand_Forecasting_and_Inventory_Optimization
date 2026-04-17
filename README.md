# Demand Forecasting & Inventory Optimisation

A data engineering project designed to reduce excess inventory, lower holding costs, free up working capital, and improve forecast accuracy — ultimately reducing stockouts and increasing product availability.

---

## Part 1 — Project Overview & Architecture

### What problem does this project solve?

Businesses that rely on physical inventory face two costly extremes: holding too much stock ties up working capital and drives up storage costs, while holding too little leads to stockouts that hurt sales and customer satisfaction. Getting the balance right requires accurate, timely data — and that's exactly what this project is built to deliver.

The system ingests operational data from source systems (such as an ERP), processes it through two parallel pipelines, and surfaces the results as interactive BI dashboards showing:

- **Current inventory levels** and replenishment needs
- **Demand forecasts** for future periods
- **Key performance indicators** to support operational decisions

### Architecture — Lambda pattern

The project follows the **Lambda architecture**, which combines two complementary processing strategies:

- **Batch pipeline** — processes large volumes of historical data on a scheduled basis (e.g. daily). Provides deep, reliable analytics and feeds the core data model.
- **Streaming pipeline** — processes events in near real-time (e.g. sales transactions, warehouse movements) as they occur. Keeps dashboards current between batch runs.

Both pipelines write to a shared **Delta Lakehouse**, from which the BI layer reads.

```
Architecture diagram: see diagram above (Section 1 visual)
```

### Technology stack

| Layer | Technology |
|---|---|
| Storage | Delta Tables (Delta Lakehouse) |
| Orchestration | Apache Airflow |
| Containerisation | Docker |
| Programming | Python |
| Data processing | PySpark, Spark SQL |
| Streaming | Apache Kafka |
| Hosting OS | Ubuntu |
| BI / Dashboards | PowerBI or Metabase (TBC) |

---

## Part 2 — Current Build Status

> The project is under active development. This section documents what has been built so far.

### Batch pipeline — Bronze layer ingestion (in progress)

Work to date focuses on the **batch processing pipeline**, and specifically on the first stage of the **medallion architecture**: ingesting raw data into the **Bronze layer** of the Delta Lakehouse.

The medallion architecture organises data into three quality tiers:

- **Bronze** — raw data, ingested as-is from source systems
- **Silver** — cleaned and validated data *(planned)*
- **Gold** — business-ready, aggregated data for reporting *(planned)*

#### What has been built

**ERP simulator (Docker container 1)**

A Python script simulates the kind of CSV file dumps that a real ERP system would produce. These files represent operational data such as inventory movements, purchase orders, or sales records. Each file is timestamped, reflecting when it was generated.

**Ingestion pipeline (Docker container 2)**

This is the main processing component. It runs the following steps in sequence:

1. **Incremental file selection** — only files dated to the current day are picked up. This ensures the pipeline is incremental rather than reprocessing everything on each run.

2. **Partitioning and enrichment** — selected files are partitioned by ingestion date, and two metadata columns are added: `processing_date` and `source_system`. This makes it easy to trace the origin and timing of every record later.

3. **Null key check** — before any data is written to the Bronze table, the merge keys (the columns that will identify unique records during an upsert) are validated. Any rows where these keys are null are removed and stored in a separate **rejected rows table** for review.

4. **Schema compatibility check** — the incoming dataset is compared against the existing Bronze table schema. If any columns are missing, mismatched, or incompatible, the affected rows are isolated and also written to the rejected rows table. Only rows that pass this check proceed.

5. **Upsert (merge) to Bronze** — validated rows are merged into the Bronze Delta table. If a record already exists (matched on the merge key), it is updated; if it is new, it is inserted. This ensures idempotency — running the pipeline more than once on the same data does not create duplicates.

6. **Monitoring table** — at the end of each run, a monitoring record is written containing: the filename, ingestion date, total row count, number of null key rows, and number of schema-incompatible rows. This provides full visibility into the health of each ingestion run.

**Orchestration**

Both containers are orchestrated by **Apache Airflow** using the `DockerOperator`. Airflow triggers the ERP simulator first, then the ingestion pipeline, ensuring the correct execution order.

```
Pipeline diagram: see diagram above (Section 2 visual)
```

### What is coming next

- Silver layer: deduplication, data type casting, and business rule validation
- Gold layer: aggregated tables for demand forecasting and inventory KPIs
- Streaming pipeline: Kafka-based real-time event ingestion
- BI dashboard: KPI views and forecast visualisations

---

*Built with Python · PySpark · Delta Tables · Apache Airflow · Docker · Ubuntu*
