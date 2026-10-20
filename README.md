# 🎵 Streaming & Incremental ETL Lakehouse with Databricks DABs

An Azure data engineering project that brings together metadata-driven ingestion, incremental data processing, and Lakehouse architecture using Azure Data Factory, ADLS Gen2, and Azure Databricks. The project focuses on building reusable ingestion pipelines, managing data quality, and organizing Databricks resources through Databricks Asset Bundles (DABs).

## 💡 Project Overview

Working with multiple source entities often means repeating ingestion logic, reprocessing existing records, and maintaining separate pipelines. To address these challenges, I developed a metadata-driven ingestion framework that uses watermark-based logic to identify new or updated records and parameterized ADF activities to reuse the same ingestion pattern across entities such as users, tracks, and streams.

The project combines Azure data orchestration with Databricks-based Lakehouse processing, with a focus on incremental loading, historical data management, and maintainable deployment workflows.

## 🏗️ Architecture & Design

<img width="1513" height="704" alt="Architecture" src="https://github.com/user-attachments/assets/17dcb4b7-9549-48ea-8c2e-e058dd7fc74d" />

### Technical Components

- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2) for the data lake storage layer.
- **Orchestration:** Azure Data Factory for parameterized pipelines and incremental ingestion.
- **Processing Engine:** Azure Databricks with PySpark for distributed data transformation.
- **Streaming:** Spark Structured Streaming concepts for processing streaming data, where configured.
- **Data Quality:** Validation rules and Delta Live Tables (DLT) expectations, where implemented.
- **DevOps:** Databricks Asset Bundles (DABs) for managing and deploying Databricks resources.

---

## 📊 Pipeline Lineage & Orchestration

**DLT Dependency Graph (DAG)**

<img width="984" height="515" alt="DLT Pipeline Dependency Graph" src="https://github.com/user-attachments/assets/95349963-5439-4374-9fbc-2b024c0eab6b" />

The architecture separates ingestion orchestration from downstream data processing. This makes pipeline dependencies easier to understand and supports a modular approach to managing data workflows.

---

## ⚙️ Technical Skills & Tools

### **Data Engineering & Orchestration**

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)
![Spark Structured Streaming](https://img.shields.io/badge/Spark_Streaming-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)

### **Infrastructure & DevOps**

![Azure ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![Databricks Asset Bundles](https://img.shields.io/badge/DABs-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=flat-square&logo=databricks&logoColor=white)

### **Languages & Frameworks**

- **Languages:** Python, PySpark, SQL (Spark SQL, T-SQL), Jinja2 for templated SQL where configured.
- **Architecture:** Medallion Architecture (Bronze, Silver, Gold), Star Schema Modeling.
- **Incremental Processing:** Watermark-based ingestion, change tracking, and SCD Type 2 concepts.
- **Data Quality:** Schema validation and DLT expectations where implemented.
- **Deployment:** Databricks Asset Bundles and Git-based version control.

### **Analytics & Visualization**

- **Power BI:** Data modeling, DAX, and Lakehouse reporting integration where the reporting assets are configured.

---

## 💎 Data Quality Guardrails

Data quality checks help prevent incomplete or invalid records from affecting downstream transformations. For example, a DLT expectation can enforce a required user identifier:

```python
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
```

This rule drops records where `user_id` is null when implemented in the applicable DLT pipeline.

---

## 🛰️ Ingestion & Monitoring (ADF + Logic Apps)

I configured an Azure Data Factory ingestion layer to coordinate data movement from source systems into the data lake. The pipeline definitions use reusable activities and parameterized datasets to support multiple source entities.

- **Incremental Ingestion:** A watermark lookup identifies the last processed value, helping the pipeline select records for the next extraction.
- **Automated Alerting:** A Logic Apps webhook integration is configured to support failure notifications.
- **Metadata-Driven Processing:** Pipeline parameters and `ForEach` activities allow the ingestion logic to be reused across configured entities.
- **Empty-File Cleanup:** A cleanup activity handles empty extraction outputs.

**ADF Pipeline Logic**

<img width="852" height="357" alt="ADF Pipeline" src="https://github.com/user-attachments/assets/9660ef3a-f7c5-45f4-a535-bc11dec61704" />

## 🔄 Metadata-Driven Incremental Ingestion (ADF)

To avoid repeatedly extracting all available source records, I implemented a watermark-based ingestion pattern in Azure Data Factory. The framework uses a saved progress value to determine which records should be processed in each run.

- **Watermark Logic:** A `Lookup` activity retrieves the previous watermark. When new data is available, the pipeline executes the relevant `Copy Data` activity and updates the control value after processing.
- **Batch Orchestration:** The `incremental_loop` pipeline uses a `ForEach` activity to apply shared ingestion logic across configured entities such as Users, Tracks, and Streams.
- **Parameterized Extraction:** Dynamic datasets and pipeline parameters allow source and destination settings to be supplied at runtime.
- **Empty-Result Handling:** The `DeleteEmptyFile` activity cleans up empty extraction outputs.

**Incremental Logic Flow**

<img width="1427" height="563" alt="Incremental Ingestion Flow" src="https://github.com/user-attachments/assets/04dc4f7f-3466-4e27-98df-a6106192111d" />

For reliable incremental processing, the watermark should advance only to a value that represents successfully processed data. Capturing a fixed extraction boundary and handling retries and partial failures are important safeguards against skipped records.

---

## 🧬 Change Data Capture (CDC) Deep Dive

Incremental loading identifies records to extract, while Change Data Capture (CDC) focuses on identifying changes such as inserts and updates. Together with historical data modeling, these patterns help maintain useful analytical history.

### SCD Type 2 Example

If a Spotify user changes their subscription from **Free** to **Premium**, overwriting the existing row would remove the previous subscription history. An SCD Type 2 model instead retains the old version and creates a new version for the updated attributes.

A typical implementation tracks:

- `is_current` — identifies the current version.
- `start_date` — records when the version becomes effective.
- `end_date` — records when the version is no longer effective.

The Databricks transformation layer can implement this historical tracking using the appropriate Delta Lake merge or supported pipeline change-application mechanism. The specific implementation should match the processing code included in the repository.

---

## 🚀 Deployment Workflow (CI/CD & DABs)

I organized the Databricks deployment configuration using **Databricks Asset Bundles (DABs)**. Bundles help manage jobs, pipelines, notebooks, and related resources as code, making deployments more repeatable and easier to maintain.

### Validate the bundle

```bash
databricks bundle validate
```

### Deploy to the development environment

```bash
databricks bundle deploy --target dev
```

### Run a configured resource

```bash
databricks bundle run <configured-resource-key> --target dev
```

Replace `<configured-resource-key>` with the resource key defined in the bundle configuration. For example, use `gold_pipeline` only if that key exists as a runnable resource in the project.

Before deployment, configure workspace authentication, environment-specific settings, and resource paths. Separate development and production configurations should point to their intended workspaces.

---

## 🛠️ Data Modeling (The "Gold" Layer)

The analytical layer is designed around a star schema to make listening activity easier to analyze in BI tools such as Power BI.

- **FactStream:** Represents listening events at the selected event grain, with measures and timestamps for analytical queries.
- **DimUser:** Stores user attributes and can preserve historical versions when SCD Type 2 is implemented.
- **DimTrack:** Stores track metadata and can track changes to descriptive attributes over time.
- **DimDate:** Provides calendar attributes for reporting by day, month, quarter, and year.

This model supports analytical questions such as listening trends over time, activity by user, and track-level engagement. The final table definitions, relationships, and measures should correspond to the actual implemented Gold-layer assets.

---

## 🌟 Professional Highlights

- **Reusable Ingestion Framework:** Designed parameterized ADF pipeline definitions that apply shared ingestion logic across multiple configured source entities.
- **Incremental Data Processing:** Used watermark-based extraction to identify new or updated records and reduce unnecessary full-table reloads.
- **Pipeline Orchestration:** Combined `Lookup`, `ForEach`, `Copy Data`, and watermark-update activities to coordinate ingestion.
- **Operational Handling:** Included empty-file cleanup and a Logic Apps webhook integration for failure notification.
- **Lakehouse Engineering:** Structured the project around Azure storage, Databricks processing, data quality, and downstream analytical modeling.
- **Deployment as Code:** Used Databricks Asset Bundles to organize and validate deployment resources.
- **Advanced SQL Concepts:** Included Jinja2-based SQL templating as an approach to reusable query generation where configured.

## 📁 Repository Structure

```text
.
├── Databricks/
│   └── spo_dab/
│       ├── databricks.yml
│       ├── pyproject.toml
│       └── resources/
├── dataset/
│   ├── AzureSql.json
│   └── Parquet_dynamic.json
├── factory/
│   └── adf-sp-project-seju.json
├── linkedService/
├── pipeline/
│   ├── incremental_ingestion.json
│   └── incremental_loop.json
├── publish_config.json
└── README.md
```

## ▶️ Getting Started

1. Clone this repository.
2. Review the ADF pipeline, dataset, and linked-service configurations.
3. Configure the source connection, storage destination, and watermark control settings for your environment.
4. Configure Databricks authentication and workspace targets.
5. Run `databricks bundle validate` to validate the bundle configuration.
6. Deploy the required resources to the development workspace.
7. Execute the configured pipelines and validate record counts, watermark progression, and failure handling.

**Security:** Keep credentials, active webhook signature URLs, and other secrets out of source control. Use managed identities or an approved secret-management mechanism, and rotate any exposed webhook signature.

---

## 🎯 Project Outcome

This project brings together reusable Azure Data Factory ingestion patterns and Databricks deployment configuration in a single data engineering repository. It demonstrates how metadata-driven orchestration, watermark-based incremental loading, and structured deployment practices can support a maintainable Lakehouse workflow.
