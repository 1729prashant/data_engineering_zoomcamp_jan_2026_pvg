<a name="top"></a>
# Table of Contents
* [1. OLTP vs OLAP](#1-oltp-vs-olap)
    * [1.1 Key Characteristics](#11-key-characteristics)
    * [1.2 Schema Design Patterns](#12-schema-design-patterns)
    * [1.3 Performance Optimization Techniques](#13-performance-optimization-techniques)
    * [1.4 Technology Examples](#14-technology-examples)
    * [1.5 Use Case Mapping](#15-use-case-mapping)
    * [1.6 Data Flow Architecture](#16-data-flow-architecture)
* [2. What is a Data Warehouse](#2-what-is-a-data-warehouse)
    * [2.1 Overview](#21-overview)
    * [2.2 Data Warehouse vs Data Lake](#22-data-warehouse-vs-data-lake)
    * [2.3 Data Warehouse Characteristics](#23-data-warehouse-characteristics)
    * [2.4 Architecture Layers](#24-architecture-layers)
    * [2.5 Common Schema Designs](#25-common-schema-designs)
    * [2.6 Reporting and Analysis Capabilities](#26-reporting-and-analysis-capabilities)
    * [2.7 Data Warehouse vs Operational Database (OLTP)](#27-data-warehouse-vs-operational-database-oltp)
    * [2.8 Modern Data Warehouse Technologies](#28-modern-data-warehouse-technologies)
    * [2.9 Data Warehouse Lifecycle](#29-data-warehouse-lifecycle)
    * [2.10 Common Use Cases](#210-common-use-cases)
* [3. BigQuery](#3-bigquery)
    * [3.1 Overview](#31-overview)
    * [3.2 Core Characteristics](#32-core-characteristics)
    * [3.3 Built-in Features](#33-built-in-features)
    * [3.4 Serverless Benefits](#34-serverless-benefits)
    * [3.5 Architecture Components](#35-architecture-components)
    * [3.6 Compute and Storage Separation](#36-compute-and-storage-separation)
    * [3.7 Query Processing Models](#37-query-processing-models)
    * [3.8 Data Ingestion Methods](#38-data-ingestion-methods)
    * [3.9 Performance Optimization Features](#39-performance-optimization-features)
    * [3.10 Data Organization](#310-data-organization)
    * [3.11 Security and Access Control](#311-security-and-access-control)
    * [3.12 Integration Ecosystem](#312-integration-ecosystem)
    * [3.13 BigQuery ML Capabilities](#313-bigquery-ml-capabilities)
    * [3.14 Geospatial Functions](#314-geospatial-functions)
    * [3.15 Comparison with Other Data Warehouses](#315-comparison-with-other-data-warehouses)
* [4. BigQuery Cost](#4-bigquery-cost)
    * [4.1 Pricing Models](#41-pricing-models)
    * [4.2 On-Demand Pricing Details](#42-on-demand-pricing-details)
    * [4.3 Flat-Rate Pricing Details](#43-flat-rate-pricing-details)
    * [4.4 Cost Optimization Strategies](#44-cost-optimization-strategies)
    * [4.5 Cost Comparison Example](#45-cost-comparison-example)
    * [4.6 Query Cost Calculation](#46-query-cost-calculation)
    * [4.7 Free Tier and Quotas](#47-free-tier-and-quotas)
    * [4.8 Cost Monitoring Tools](#48-cost-monitoring-tools)
    * [4.9 Hidden Costs to Watch](#49-hidden-costs-to-watch)
* [5. BigQuery Partitions and Clustering](#5-bigquery-partitions-and-clustering)
    * [5.1 Partitioning Overview](#51-partitioning-overview)
    * [5.2 Partition Types](#52-partition-types)
    * [5.3 Time-Unit Partition Granularity](#53-time-unit-partition-granularity)
    * [5.4 Partitioning Examples](#54-partitioning-examples)
    * [5.5 Clustering Overview](#55-clustering-overview)
    * [5.6 Clustering Column Requirements](#56-clustering-column-requirements)
    * [5.7 Clustering Benefits](#57-clustering-benefits)
    * [5.8 Clustering Example](#58-clustering-example)
    * [5.9 Partitioning vs Clustering](#59-partitioning-vs-clustering)
    * [5.10 When to Use Clustering Over Partitioning](#510-when-to-use-clustering-over-partitioning)
    * [5.11 Combining Partitioning and Clustering](#511-combining-partitioning-and-clustering)
    * [5.12 Automatic Reclustering](#512-automatic-reclustering)
    * [5.13 Clustering Column Order Best Practices](#513-clustering-column-order-best-practices)
	    * [5.13.1 Example Column Order](#5131-example-column-order)
    * [5.14 Performance Improvement Thresholds](#514-performance-improvement-thresholds)
    * [5.15 Query Pruning Metrics](#515-query-pruning-metrics)
    * [5.16 Common Patterns](#516-common-patterns)
    * [5.17 Resource Reference](#517-resource-reference)
* [6. BigQuery Best Practices](#6-bigquery-best-practices)
    * [6.1 Cost Reduction](#61-cost-reduction)
    * [6.2 Query Performance - Table Design](#62-query-performance---table-design)
    * [6.3 Query Performance - External Data](#63-query-performance---external-data)
    * [6.4 Query Performance - Query Optimization](#64-query-performance---query-optimization)
    * [6.5 Partitioning and Clustering Best Practices](#65-partitioning-and-clustering-best-practices)
    * [6.6 JOIN Optimization](#66-join-optimization)
        * [6.6.1 JOIN Order Example](#661-join-order-example)
    * [6.7 WITH Clause Best Practices](#67-with-clause-best-practices)
        * [6.7.1 WITH Clause Example](#671-with-clause-example)
    * [6.8 Nested and Repeated Columns](#68-nested-and-repeated-columns)
        * [6.8.1 Nested Column Example](#681-nested-column-example)
    * [6.9 Approximate Aggregation Functions](#69-approximate-aggregation-functions)
    * [6.10 JavaScript UDF Alternatives](#610-javascript-udf-alternatives)
    * [6.11 Anti-Patterns to Avoid](#611-anti-patterns-to-avoid)
    * [6.12 Query Execution Order for Performance](#612-query-execution-order-for-performance)
    * [6.13 Monitoring and Optimization](#613-monitoring-and-optimization)
* [7. ML in BigQuery](#7-ml-in-bigquery)
    * [Overview](#overview)
    * [BigQuery ML Workflow](#bigquery-ml-workflow)

<a name="1-oltp-vs-olap"></a>

# 1. OLTP vs OLAP 

| Aspect | OLTP | OLAP |
|--------|------|------|
| **Purpose** | Control and run essential business operations in real time | Plan, solve problems, support decisions, discover hidden insights |
| **Data Updates** | Short, fast updates initiated by user | Data periodically refreshed with scheduled, long-running batch jobs |
| **Database Design** | Normalized databases for efficiency | Denormalized databases for analysis |
| **Space Requirements** | Generally small if historical data is archived | Generally large due to aggregating large datasets |
| **Backup and Recovery** | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| **Productivity** | Increases productivity of end users | Increases productivity of business managers, data analysts, and executives |
| **Data View** | Lists day-to-day business transactions | Multi-dimensional view of enterprise data |
| **User Examples** | Customer-facing personnel, clerks, online shoppers | Knowledge workers such as data analysts, business analysts, and executives |

<a name="11-key-characteristics"></a>

## 1.1 Key Characteristics

| Characteristic | OLTP | OLAP |
|----------------|------|------|
| **Transaction Type** | INSERT, UPDATE, DELETE operations | SELECT queries with aggregations |
| **Response Time** | Milliseconds | Seconds to minutes |
| **Data Model** | Highly normalized (3NF, BCNF) | Denormalized (Star schema, Snowflake schema) |
| **Query Complexity** | Simple queries accessing few records | Complex queries scanning millions of records |
| **Concurrency** | Thousands of concurrent transactions | Limited concurrent users |
| **Data Scope** | Current, operational data | Historical, aggregated data |
| **Index Strategy** | B-tree indexes on primary/foreign keys | Bitmap indexes, columnar indexes |
| **Storage Optimization** | Row-oriented storage | Column-oriented storage |

<a name="12-schema-design-patterns"></a>

## 1.2 Schema Design Patterns

| Pattern | OLTP Example | OLAP Example |
|---------|--------------|--------------|
| **Customer Data** | `Customers(id, name, email)`<br>`Addresses(id, customer_id, street, city)` | `DimCustomer(id, name, email, street, city, region, segment)` |
| **Product Data** | `Products(id, name, category_id)`<br>`Categories(id, name)` | `DimProduct(id, name, category, subcategory, brand)` |
| **Transaction Data** | `Orders(id, customer_id, date)`<br>`OrderItems(id, order_id, product_id, qty, price)` | `FactSales(date_key, customer_key, product_key, quantity, revenue, cost, profit)` |

<a name="13-performance-optimization-techniques"></a>

## 1.3 Performance Optimization Techniques

| Technique | OLTP | OLAP |
|-----------|------|------|
| **Indexing** | B-tree indexes on foreign keys | Bitmap indexes, inverted indexes |
| **Partitioning** | Range/hash partitioning by date or ID | Partitioning by time period (daily, monthly) |
| **Caching** | Application-level caching (Redis, Memcached) | Materialized views, result caching |
| **Scaling** | Vertical scaling, read replicas, sharding | Horizontal scaling, MPP architecture |
| **Compression** | Minimal (optimizes write speed) | Heavy compression (optimizes storage/scan) |
| **Query Optimization** | Index optimization, query plan tuning | Parallel processing, columnar scans |

<a name="14-technology-examples"></a>

## 1.4 Technology Examples

| Category | OLTP Technologies | OLAP Technologies |
|----------|-------------------|-------------------|
| **Relational** | PostgreSQL, MySQL, Oracle, SQL Server | BigQuery, Snowflake, Redshift, Synapse |
| **Distributed** | CockroachDB, Cassandra, DynamoDB | ClickHouse, Druid, Vertica |
| **In-Memory** | Redis, Memcached, VoltDB | SAP HANA, MemSQL |
| **Cloud-Native** | Aurora, Cloud SQL, Azure SQL | BigQuery, Snowflake, Databricks |

<a name="15-use-case-mapping"></a>

## 1.5 Use Case Mapping

| Use Case | System Type | Rationale |
|----------|-------------|-----------|
| E-commerce checkout | OLTP | Requires ACID guarantees, low latency, high concurrency |
| Banking transactions | OLTP | Critical data consistency, real-time processing |
| Inventory management | OLTP | Frequent updates, immediate consistency needed |
| Sales reporting dashboard | OLAP | Complex aggregations across time, products, regions |
| Customer lifetime value analysis | OLAP | Historical data analysis, predictive modeling |
| Business forecasting | OLAP | Trend analysis, what-if scenarios |
| Fraud detection (real-time) | OLTP | Immediate transaction validation |
| Fraud pattern analysis | OLAP | Historical pattern recognition |

<a name="16-data-flow-architecture"></a>

## 1.6 Data Flow Architecture

| Component | OLTP Role | OLAP Role |
|-----------|-----------|-----------|
| **Source Systems** | Primary data source | N/A |
| **ETL/ELT Pipeline** | Data extraction source | Data loading destination |
| **Data Freshness** | Real-time (sub-second) | Batch (hourly, daily, near real-time) |
| **Data Retention** | Recent data (90 days - 2 years) | Historical data (unlimited) |
| **Typical Flow** | Application → OLTP DB | OLTP DB → ETL → Data Warehouse (OLAP) → BI Tools |

[↑ Back to Top](#top)
<a name="2-what-is-a-data-warehouse"></a>

# 2. What is a Data Warehouse

<a name="21-overview"></a>

## 2.1 Overview

| Aspect | Description |
|--------|-------------|
| **Definition** | A data warehouse is a centralized repository that stores integrated data from multiple sources, optimized for query and analysis rather than transaction processing |
| **Primary Function** | OLAP solution used for reporting and data analysis |
| **Architecture Type** | Subject-oriented, integrated, time-variant, and non-volatile data storage system |

<a name="22-data-warehouse-vs-data-lake"></a>

## 2.2 Data Warehouse vs Data Lake

| Feature | Data Warehouse | Data Lake |
|---------|----------------|-----------|
| **Data Structure** | Structured, processed data with defined schema (schema-on-write) | Raw, unstructured, semi-structured, and structured data (schema-on-read) |
| **Data Processing** | ETL (Extract, Transform, Load) - data transformed before storage | ELT (Extract, Load, Transform) - data transformed after storage |
| **Storage Format** | Relational tables with optimized columnar storage | Object storage (Parquet, Avro, JSON, CSV, images, videos, logs) |
| **Users** | Business analysts, data analysts, executives | Data scientists, data engineers, ML engineers, analysts |
| **Query Performance** | Optimized for fast SQL queries and aggregations | Varies - slower for ad-hoc queries, faster for big data processing |
| **Cost** | Higher cost per GB due to processing and optimization | Lower cost per GB for raw storage |
| **Data Quality** | High - cleaned, validated, conformed data | Variable - raw data may contain errors, duplicates |
| **Use Cases** | Business intelligence, reporting, dashboards, KPI tracking | Machine learning, data exploration, advanced analytics, data archival |
| **Schema** | Predefined schema (star, snowflake) | Flexible schema or schema-less |
| **Governance** | Strong data governance, metadata management | Requires additional governance layers |
| **Agility** | Less agile - schema changes are complex | Highly agile - store first, define schema later |
| **Examples** | BigQuery, Snowflake, Redshift, Synapse | AWS S3 + Athena, Azure Data Lake, Google Cloud Storage |

<a name="23-data-warehouse-characteristics"></a>

## 2.3 Data Warehouse Characteristics

| Characteristic | Description | Example |
|----------------|-------------|---------|
| **Subject-Oriented** | Organized around key business subjects rather than applications | Sales, Customers, Products, Inventory (not by application like "ERP" or "CRM") |
| **Integrated** | Data from multiple sources consolidated with consistent naming, formats, and encoding | Customer data unified from CRM, ERP, web analytics with standardized formats |
| **Time-Variant** | Contains historical data with time dimensions for trend analysis | Sales data from 2015-2025, snapshot of data at different time periods |
| **Non-Volatile** | Data is read-only; not updated or deleted, only loaded and accessed | Once loaded, transactional records remain unchanged for historical accuracy |

<a name="24-architecture-layers"></a>

## 2.4 Architecture Layers

| Layer | Purpose | Components |
|-------|---------|------------|
| **Data Sources** | Origin systems providing raw data | OLTP databases, CRM, ERP, flat files, APIs, streaming data |
| **ETL/ELT Layer** | Data extraction, transformation, and loading | Apache Airflow, dbt, Talend, Informatica, Fivetran, Stitch |
| **Storage Layer** | Physical data warehouse storage | BigQuery, Snowflake, Redshift, Azure Synapse |
| **Data Model Layer** | Logical organization of data | Fact tables, dimension tables (star/snowflake schema) |
| **Presentation Layer** | Data access and consumption | BI tools (Tableau, Looker, Power BI), SQL clients, APIs |

<a name="25-common-schema-designs"></a>

## 2.5 Common Schema Designs

| Schema Type | Structure | Advantages | Disadvantages | Use Case |
|-------------|-----------|------------|---------------|----------|
| **Star Schema** | Central fact table connected directly to dimension tables | Simple queries, fast performance, easy to understand | Data redundancy in dimensions | Most common for BI and reporting |
| **Snowflake Schema** | Normalized dimension tables (dimensions broken into sub-dimensions) | Reduced data redundancy, easier maintenance | More complex joins, slightly slower queries | When storage optimization is critical |
| **Galaxy Schema** | Multiple fact tables sharing dimension tables | Supports complex analytical requirements | Most complex design and queries | Enterprise-wide data warehouses |

<a name="26-reporting-and-analysis-capabilities"></a>

## 2.6 Reporting and Analysis Capabilities

| Capability | Description | Example Query |
|------------|-------------|---------------|
| **Aggregation** | Sum, average, count, min, max across dimensions | Total revenue by region and quarter |
| **Drill-Down** | Navigate from summary to detailed data | Year → Quarter → Month → Day → Transaction |
| **Drill-Up** | Navigate from detailed to summary data | Product → Product Category → Product Line |
| **Slice and Dice** | View data from different perspectives | Sales by Region (slice), then by Product and Time (dice) |
| **Pivot** | Rotate data axes for different views | Rows: Products, Columns: Regions → Rows: Regions, Columns: Products |
| **Time-Series Analysis** | Trend analysis over time periods | Year-over-year growth, moving averages, seasonality |

<a name="27-data-warehouse-vs-operational-database-oltp"></a>

## 2.7 Data Warehouse vs Operational Database (OLTP)

| Aspect | Data Warehouse (OLAP) | Operational Database (OLTP) |
|--------|----------------------|----------------------------|
| **Workload** | Read-heavy, complex analytical queries | Write-heavy, simple transactional queries |
| **Query Pattern** | Few users running complex queries on large datasets | Many users running simple queries on small datasets |
| **Data Age** | Historical data (months to years) | Current data (hours to days) |
| **Response Time** | Seconds to minutes acceptable | Milliseconds required |
| **Data Volume per Query** | Millions to billions of rows | Tens to thousands of rows |
| **Optimization Goal** | Query throughput and scan efficiency | Transaction throughput and ACID compliance |

<a name="28-modern-data-warehouse-technologies"></a>

## 2.8 Modern Data Warehouse Technologies

| Technology | Provider | Key Features | Best For |
|------------|----------|--------------|----------|
| **BigQuery** | Google Cloud | Serverless, automatic scaling, ML integration, pay-per-query | Large-scale analytics, ad-hoc queries, ML workflows |
| **Snowflake** | Snowflake Inc. | Multi-cloud, separate compute/storage, instant scaling | Enterprises needing flexibility and performance |
| **Redshift** | AWS | Columnar storage, massively parallel processing, AWS integration | AWS-native architectures |
| **Synapse Analytics** | Microsoft Azure | Unified analytics, Spark integration, data lake integration | Azure ecosystems, hybrid architectures |
| **Databricks** | Databricks | Lakehouse architecture, Spark-based, Delta Lake | Data engineering + data science workloads |

<a name="29-data-warehouse-lifecycle"></a>

## 2.9 Data Warehouse Lifecycle

| Phase | Activities | Outputs |
|-------|------------|---------|
| **Requirements Gathering** | Identify business questions, KPIs, reporting needs | Business requirements document, data dictionary |
| **Design** | Create dimensional models, define ETL processes | Star/snowflake schemas, ETL specifications |
| **Development** | Build ETL pipelines, create tables, develop transformations | Production-ready data pipelines, data models |
| **Testing** | Validate data quality, test queries, performance tuning | Test reports, performance benchmarks |
| **Deployment** | Release to production, schedule jobs, monitor | Live data warehouse, operational dashboards |
| **Maintenance** | Monitor performance, handle schema changes, optimize | Updated models, improved performance |

<a name="210-common-use-cases"></a>

## 2.10 Common Use Cases

| Use Case | Description | Typical Queries |
|----------|-------------|-----------------|
| **Sales Analytics** | Track revenue, identify trends, forecast | Top products by revenue, sales by region and time |
| **Customer Analytics** | Customer segmentation, lifetime value, churn analysis | Customer retention rate, RFM analysis |
| **Financial Reporting** | P&L statements, budget vs actual, cost analysis | Monthly revenue, expense breakdown by department |
| **Supply Chain** | Inventory levels, supplier performance, demand forecasting | Stock turnover rate, supplier lead times |
| **Marketing Analytics** | Campaign effectiveness, ROI, attribution | Conversion rates by channel, marketing spend ROI |
| **Operational Metrics** | KPI tracking, SLA monitoring, efficiency metrics | Average handling time, first response time |


[↑ Back to Top](#top)
<a name="3-bigquery"></a>

# 3. BigQuery

<a name="31-overview"></a>

## 3.1 Overview

| Aspect | Description |
|--------|-------------|
| **Definition** | Serverless, highly scalable, and cost-effective multi-cloud data warehouse designed for business agility |
| **Provider** | Google Cloud Platform (GCP) |
| **Architecture** | Separation of compute and storage for independent scaling |

<a name="32-core-characteristics"></a>

## 3.2 Core Characteristics

| Feature | Description |
|---------|-------------|
| **Serverless** | No servers to manage or database software to install - fully managed by Google |
| **Infrastructure Management** | Software and infrastructure including scalability and high-availability are handled automatically |
| **Compute-Storage Separation** | Compute engine (query processing) is separated from storage, allowing independent scaling |
| **Query Engine** | Dremel-based distributed SQL query engine |
| **Storage Format** | Columnar storage format (Capacitor) optimized for analytics |

<a name="33-built-in-features"></a>

## 3.3 Built-in Features

| Feature | Description | Use Case |
|---------|-------------|----------|
| **Machine Learning (BigQuery ML)** | Create and execute ML models using SQL queries | Predict customer churn, forecast sales, product recommendations |
| **Geospatial Analysis** | Built-in geography data type and GIS functions | Location-based analytics, route optimization, spatial joins |
| **Business Intelligence (BI Engine)** | In-memory analysis service for interactive dashboards | Real-time dashboards with sub-second query response |
| **Data Transfer Service** | Automated data ingestion from SaaS applications | Import data from Google Ads, YouTube, Google Analytics |
| **Streaming Inserts** | Real-time data ingestion | Live dashboards, real-time monitoring |
| **Federated Queries** | Query external data sources without loading | Query Cloud Storage, Bigtable, Spanner directly |
| **Data Studio Integration** | Native integration with Google Data Studio | Create reports and dashboards |
| **APIs and Client Libraries** | REST API, Python, Java, Node.js, Go SDKs | Programmatic access and automation |

<a name="34-serverless-benefits"></a>

## 3.4 Serverless Benefits

| Benefit | Description | Impact |
|---------|-------------|--------|
| **No Infrastructure Management** | No provisioning, patching, or maintenance required | Focus on analysis, not infrastructure |
| **Automatic Scaling** | Scales compute resources automatically based on query demands | Handle workload spikes without manual intervention |
| **High Availability** | Built-in replication and fault tolerance | 99.99% SLA without configuration |
| **Zero Downtime** | No maintenance windows or downtime | 24/7 availability for queries |
| **Pay-per-Use** | Pay only for storage and queries executed | No idle resource costs |

<a name="35-architecture-components"></a>

## 3.5 Architecture Components

| Component | Function | Details |
|-----------|----------|---------|
| **Dremel Engine** | Query execution engine | Massively parallel processing, distributed execution |
| **Colossus** | Distributed storage system | Google's next-generation distributed file system |
| **Capacitor** | Columnar storage format | Optimized for analytical queries and compression |
| **Borg** | Cluster management | Google's container orchestration (predecessor to Kubernetes) |
| **Jupiter Network** | High-speed networking | Petabit-scale network fabric for fast data shuffling |

<a name="36-compute-and-storage-separation"></a>

## 3.6 Compute and Storage Separation

| Aspect | Traditional Architecture | BigQuery Architecture |
|--------|-------------------------|----------------------|
| **Scaling** | Scale compute and storage together | Scale compute and storage independently |
| **Resource Allocation** | Fixed cluster size | Dynamic allocation based on query needs |
| **Cost Efficiency** | Pay for provisioned capacity | Pay only for actual usage |
| **Performance** | Limited by cluster size | Virtually unlimited compute for queries |
| **Storage Growth** | Requires cluster expansion | Storage grows independently of compute |
| **Query Concurrency** | Limited by cluster resources | Thousands of concurrent queries supported |

<a name="37-query-processing-models"></a>

## 3.7 Query Processing Models

| Model | Description | Pricing | Use Case |
|-------|-------------|---------|----------|
| **On-Demand** | Pay per TB of data processed | $6.25 per TB (first 1 TB/month free) | Variable workloads, ad-hoc analysis |
| **Flat-Rate (Slots)** | Purchase dedicated query processing capacity | Starting at $2,000/month for 100 slots | Predictable workloads, cost control |
| **Flex Slots** | Purchase slots for 60-second minimum commitments | $0.04 per slot-hour | Burst workloads, short-term needs |
| **Reservations** | Assign workloads to specific slot pools | Part of flat-rate pricing | Workload isolation, priority management |

<a name="38-data-ingestion-methods"></a>

## 3.8 Data Ingestion Methods

| Method | Type | Latency | Use Case |
|--------|------|---------|----------|
| **Batch Load** | Bulk data import (CSV, JSON, Avro, Parquet, ORC) | Minutes | Historical data loads, daily ETL |
| **Streaming Insert** | Real-time row-by-row ingestion | Seconds | Live dashboards, real-time analytics |
| **Storage Write API** | High-throughput streaming | Sub-second | High-volume streaming applications |
| **Data Transfer Service** | Automated scheduled transfers | Scheduled | SaaS data imports (Google Ads, GA4) |
| **Federated Queries** | Query without loading | Real-time | External data sources (Cloud Storage, Sheets) |
| **BigQuery Data Transfer** | Managed connectors | Scheduled | Amazon S3, Teradata, Redshift migrations |

<a name="39-performance-optimization-features"></a>

## 3.9 Performance Optimization Features

| Feature | Description | Benefit |
|---------|-------------|---------|
| **Partitioning** | Divide tables into segments (by date, timestamp, integer range) | Reduce query costs by scanning only relevant partitions |
| **Clustering** | Sort data within partitions by specified columns | Improve query performance through data locality |
| **Materialized Views** | Precomputed query results automatically maintained | Faster queries for repeated aggregations |
| **BI Engine** | In-memory caching layer | Sub-second response for dashboard queries |
| **Query Cache** | Automatic caching of query results (24 hours) | Free, instant results for repeated queries |
| **Approximate Aggregation** | Functions like APPROX_COUNT_DISTINCT | Faster queries with acceptable accuracy trade-offs |

<a name="310-data-organization"></a>

## 3.10 Data Organization

| Concept | Description | Structure |
|---------|-------------|-----------|
| **Project** | Top-level container for GCP resources | Billing and access control boundary |
| **Dataset** | Collection of tables and views | Schema-level organization, geographic location |
| **Table** | Structured data storage | Standard, partitioned, clustered, external |
| **View** | Virtual table defined by SQL query | Logical, materialized |
| **Routine** | Stored procedures and functions | User-defined functions (UDF), stored procedures |

<a name="311-security-and-access-control"></a>

## 3.11 Security and Access Control

| Feature | Description | Implementation |
|---------|-------------|----------------|
| **IAM Roles** | Identity and Access Management | Project, dataset, table, column-level permissions |
| **Authorized Views** | Restrict access to specific rows/columns | Create views with row-level security |
| **Column-Level Security** | Policy tags for sensitive data | Data Catalog policy tags |
| **Encryption** | Data encrypted at rest and in transit | Automatic, customer-managed keys (CMEK) available |
| **VPC Service Controls** | Network perimeter security | Prevent data exfiltration |
| **Audit Logs** | Track all data access and modifications | Cloud Logging integration |
| **Data Loss Prevention (DLP)** | Detect and mask sensitive data | Automatic PII detection |

<a name="312-integration-ecosystem"></a>

## 3.12 Integration Ecosystem

| Category | Tools/Services | Purpose |
|----------|----------------|---------|
| **Data Integration** | Cloud Data Fusion, Dataflow, Dataproc | ETL/ELT workflows |
| **Business Intelligence** | Looker, Data Studio, Tableau, Power BI | Visualization and reporting |
| **Data Science** | Vertex AI, Colab, Jupyter notebooks | ML model development |
| **Orchestration** | Cloud Composer (Apache Airflow), Cloud Scheduler | Workflow automation |
| **Data Catalog** | Dataplex, Data Catalog | Metadata management, data discovery |
| **Storage** | Cloud Storage, Cloud SQL, Bigtable, Spanner | Federated queries, data sources |

<a name="313-bigquery-ml-capabilities"></a>

## 3.13 BigQuery ML Capabilities

| Model Type | SQL Function | Use Case |
|------------|--------------|----------|
| **Linear Regression** | `CREATE MODEL ... OPTIONS(model_type='linear_reg')` | Sales forecasting, price prediction |
| **Logistic Regression** | `CREATE MODEL ... OPTIONS(model_type='logistic_reg')` | Binary classification, churn prediction |
| **K-Means Clustering** | `CREATE MODEL ... OPTIONS(model_type='kmeans')` | Customer segmentation |
| **Time Series** | `CREATE MODEL ... OPTIONS(model_type='arima_plus')` | Demand forecasting, trend analysis |
| **Matrix Factorization** | `CREATE MODEL ... OPTIONS(model_type='matrix_factorization')` | Recommendation systems |
| **Deep Neural Networks** | `CREATE MODEL ... OPTIONS(model_type='dnn_classifier')` | Complex classification tasks |
| **Boosted Trees** | `CREATE MODEL ... OPTIONS(model_type='boosted_tree_classifier')` | High-accuracy classification |
| **AutoML Tables** | `CREATE MODEL ... OPTIONS(model_type='automl_classifier')` | Automated model selection |
| **Imported Models** | TensorFlow, ONNX model import | Custom models from Vertex AI |

<a name="314-geospatial-functions"></a>

## 3.14 Geospatial Functions

| Function Category | Examples | Use Case |
|-------------------|----------|----------|
| **Constructors** | `ST_GEOGPOINT()`, `ST_GEOGFROMTEXT()` | Create geography objects |
| **Accessors** | `ST_X()`, `ST_Y()`, `ST_BOUNDARY()` | Extract geography properties |
| **Transformations** | `ST_BUFFER()`, `ST_CENTROID()`, `ST_UNION()` | Modify geography objects |
| **Predicates** | `ST_CONTAINS()`, `ST_INTERSECTS()`, `ST_WITHIN()` | Spatial relationships |
| **Measurements** | `ST_DISTANCE()`, `ST_AREA()`, `ST_LENGTH()` | Calculate distances and areas |
| **Analysis** | `ST_CLUSTERDBSCAN()`, `ST_CONVEXHULL()` | Spatial clustering and analysis |

<a name="315-comparison-with-other-data-warehouses"></a>

## 3.15 Comparison with Other Data Warehouses

| Feature | BigQuery | Snowflake | Redshift |
|---------|----------|-----------|----------|
| **Architecture** | Serverless, auto-scaling | Multi-cluster, auto-scaling | Provisioned clusters |
| **Pricing Model** | Pay-per-query or flat-rate slots | Per-second compute + storage | Hourly cluster pricing |
| **Setup** | Zero setup, instant queries | Minimal setup | Cluster provisioning required |
| **Scaling** | Automatic, transparent | Manual or auto-scaling | Manual resize or concurrency scaling |
| **Cloud Support** | Google Cloud only | Multi-cloud (AWS, Azure, GCP) | AWS only |
| **ML Integration** | Native BigQuery ML | Snowpark ML | SageMaker integration |
| **Free Tier** | 1 TB queries/month, 10 GB storage | $400 credit | 2-month trial |

[↑ Back to Top](#top)
<a name="4-bigquery-cost"></a>

# 4. BigQuery Cost

<a name="41-pricing-models"></a>

## 4.1 Pricing Models

| Model | Description | Cost | Best For |
|-------|-------------|------|----------|
| **On-Demand Pricing** | Pay per TB of data processed by queries | $6.25 per TB (after first 1 TB free per month) | Variable/unpredictable workloads, ad-hoc analysis, getting started |
| **Flat-Rate Pricing** | Pay for dedicated query processing capacity (slots) | Starting at $2,000/month for 100 slots | Predictable workloads, high query volumes, cost control |
| **Flex Slots** | Purchase slots with 60-second minimum commitment | $0.04 per slot-hour | Burst workloads, short-term capacity needs |

<a name="42-on-demand-pricing-details"></a>

## 4.2 On-Demand Pricing Details

| Component | Cost | Notes |
|-----------|------|-------|
| **Query Processing** | $6.25 per TB scanned | First 1 TB per month is free |
| **Storage (Active)** | $0.02 per GB per month | First 10 GB per month is free |
| **Storage (Long-term)** | $0.01 per GB per month | Tables not modified for 90 consecutive days (50% discount) |
| **Streaming Inserts** | $0.010 per 200 MB | First 2 TB per month is free |
| **BigQuery ML** | Same as query pricing | Model training counted as query bytes processed |
| **BI Engine** | $0.06 per GB per hour | In-memory analysis capacity |

<a name="43-flat-rate-pricing-details"></a>

## 4.3 Flat-Rate Pricing Details

| Slots | Monthly Cost | Equivalent On-Demand Processing | Break-Even Point |
|-------|--------------|--------------------------------|------------------|
| 100 slots | $2,000 | ~320 TB per month | 320 TB processed |
| 500 slots | $10,000 | ~1,600 TB per month | 1.6 PB processed |
| 2,000 slots | $40,000 | ~6,400 TB per month | 6.4 PB processed |

<a name="44-cost-optimization-strategies"></a>

## 4.4 Cost Optimization Strategies

| Strategy | Description | Potential Savings |
|----------|-------------|-------------------|
| **Partitioning** | Use date/timestamp partitioning to scan only relevant data | 50-90% reduction in query costs |
| **Clustering** | Cluster tables by frequently filtered columns | 30-70% reduction in query costs |
| **SELECT Specific Columns** | Avoid `SELECT *`, query only needed columns | Proportional to columns selected |
| **Use Query Cache** | Leverage 24-hour cached results (free) | 100% for repeated queries |
| **Materialized Views** | Pre-aggregate frequently queried data | 70-95% reduction for aggregations |
| **Preview Data** | Use `LIMIT` or table preview (free) for exploration | 100% for data inspection |
| **Estimate Before Running** | Check query validator for bytes to be processed | Prevent expensive mistakes |
| **Approximate Aggregations** | Use `APPROX_COUNT_DISTINCT()` instead of `COUNT(DISTINCT)` | 10-30% reduction |
| **Long-term Storage** | Tables unused for 90+ days automatically discounted | 50% storage cost reduction |
| **Flat-Rate for Heavy Use** | Switch to slots if processing > 320 TB/month | 30-60% savings at scale |

<a name="45-cost-comparison-example"></a>

## 4.5 Cost Comparison Example

| Scenario | Data Processed | On-Demand Cost | Flat-Rate Cost (100 slots) | Recommended Model |
|----------|----------------|----------------|---------------------------|-------------------|
| Light Usage | 50 TB/month | $312.50 | $2,000 | On-Demand |
| Medium Usage | 200 TB/month | $1,250 | $2,000 | On-Demand |
| Heavy Usage | 400 TB/month | $2,500 | $2,000 | Flat-Rate |
| Very Heavy Usage | 1 PB/month | $6,250 | $2,000 | Flat-Rate |

<a name="46-query-cost-calculation"></a>

## 4.6 Query Cost Calculation

| Factor | Impact on Cost |
|--------|----------------|
| **Bytes Scanned** | Primary cost driver - only scanned data is charged |
| **Columns Selected** | Columnar storage means fewer columns = lower cost |
| **Partitions Pruned** | Partition filters reduce scanned data |
| **Clustering Applied** | Clustered columns reduce data scanned via block pruning |
| **Query Complexity** | No impact - compute is included in per-TB pricing |
| **Result Size** | No impact - only input data scanned is charged |

<a name="47-free-tier-and-quotas"></a>

## 4.7 Free Tier and Quotas

| Resource | Free Tier | Quota Limit |
|----------|-----------|-------------|
| **Query Processing** | First 1 TB per month | No limit (on-demand) |
| **Storage** | First 10 GB per month | No limit |
| **Streaming Inserts** | First 2 TB per month | 100,000 rows per second per project |
| **Batch Loads** | Free (unlimited) | 1,500 jobs per table per day |
| **Concurrent Queries** | N/A | 100 interactive + 100 batch (on-demand) |
| **Slots (On-Demand)** | Shared pool | 2,000 slots per project (default) |

<a name="48-cost-monitoring-tools"></a>

## 4.8 Cost Monitoring Tools

| Tool | Purpose | Access |
|------|---------|--------|
| **Query Validator** | Estimate bytes processed before running | BigQuery Console (before execution) |
| **Query History** | View past query costs and bytes processed | BigQuery Console → Query History |
| **Cost Controls** | Set custom cost controls and alerts | BigQuery Console → Cost Controls |
| **Cloud Billing Reports** | Analyze BigQuery spending trends | GCP Console → Billing |
| **Slot Utilization Dashboard** | Monitor slot usage (flat-rate) | BigQuery Admin Console |
| **Information Schema** | Query metadata about jobs and costs | `INFORMATION_SCHEMA.JOBS` |

<a name="49-hidden-costs-to-watch"></a>

## 4.9 Hidden Costs to Watch

| Cost Factor | Description | Mitigation |
|-------------|-------------|------------|
| **Cross-Region Queries** | Querying datasets in different regions incurs egress charges | Keep datasets in same region |
| **Large Result Sets** | Exporting massive results to Cloud Storage | Use table destinations instead |
| **Repeated Non-Cached Queries** | Cache only lasts 24 hours | Use materialized views for frequent queries |
| **Inefficient Queries** | Full table scans without filters | Always use WHERE clauses on partitioned columns |
| **Streaming Buffer** | Streaming data not immediately eligible for long-term discount | Batch loads when possible |

[↑ Back to Top](#top)
<a name="5-bigquery-partitions-and-clustering"></a>

# 5. BigQuery Partitions and Clustering

<a name="51-partitioning-overview"></a>

## 5.1 Partitioning Overview

| Aspect | Description |
|--------|-------------|
| **Purpose** | Divide tables into segments to reduce query costs and improve performance |
| **Partition Limit** | Maximum 4,000 partitions per table |
| **Best For** | Tables > 1 GB with time-based or range-based query patterns |
| **Cost Impact** | Only scans relevant partitions, reducing bytes processed |

<a name="52-partition-types"></a>

## 5.2 Partition Types

| Partition Type | Description | Use Case |
|----------------|-------------|----------|
| **Time-Unit Column** | Partition based on DATE, TIMESTAMP, or DATETIME column | Event data, logs, transactions with date filters |
| **Ingestion Time** | Partition based on data load time (`_PARTITIONTIME` pseudo-column) | When data lacks timestamp column, streaming inserts |
| **Integer Range** | Partition based on integer column ranges | Customer ID, product ID, year as integer |

<a name="53-time-unit-partition-granularity"></a>

## 5.3 Time-Unit Partition Granularity

| Granularity | Partition Size | Use Case | Example |
|-------------|----------------|----------|---------|
| **Hourly** | 1 hour per partition | High-volume streaming data, real-time analytics | IoT sensor data, clickstream events |
| **Daily** | 1 day per partition (default) | Most common use case | Transaction logs, daily sales data |
| **Monthly** | 1 month per partition | Lower query frequency, historical analysis | Monthly reports, archived data |
| **Yearly** | 1 year per partition | Long-term storage, infrequent access | Multi-year historical archives |

<a name="54-partitioning-examples"></a>

## 5.4 Partitioning Examples

| Partition Type | SQL Example |
|----------------|-------------|
| **Daily Partition (Column)** | `CREATE TABLE dataset.table (date DATE, ...) PARTITION BY DATE(date)` |
| **Hourly Partition (Column)** | `CREATE TABLE dataset.table (ts TIMESTAMP, ...) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)` |
| **Monthly Partition (Column)** | `CREATE TABLE dataset.table (date DATE, ...) PARTITION BY DATE_TRUNC(date, MONTH)` |
| **Ingestion Time** | `CREATE TABLE dataset.table (...) PARTITION BY _PARTITIONTIME` |
| **Integer Range** | `CREATE TABLE dataset.table (customer_id INT64, ...) PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 100000, 1000))` |

<a name="55-clustering-overview"></a>

## 5.5 Clustering Overview

| Aspect | Description |
|--------|-------------|
| **Purpose** | Sort and colocate data within partitions or tables for better query performance |
| **Column Limit** | Up to 4 clustering columns per table |
| **Column Order** | Important - determines sort order (specify most filtered columns first) |
| **Automatic Reclustering** | BigQuery automatically maintains clustering as data is added |
| **Best For** | Tables > 1 GB with frequent filter or aggregate queries on specific columns |

<a name="56-clustering-column-requirements"></a>

## 5.6 Clustering Column Requirements

| Requirement | Details |
|-------------|---------|
| **Column Types** | DATE, BOOL, GEOGRAPHY, INT64, NUMERIC, BIGNUMERIC, STRING, TIMESTAMP, DATETIME |
| **Column Structure** | Must be top-level, non-repeated columns (no ARRAY or STRUCT fields) |
| **Column Order** | First column most important, determines primary sort order |
| **Maximum Columns** | 4 clustering columns maximum |

<a name="57-clustering-benefits"></a>

## 5.7 Clustering Benefits

| Query Type | Performance Improvement | Example |
|------------|------------------------|---------|
| **Filter Queries** | Scans only relevant data blocks | `WHERE customer_id = 12345 AND region = 'US'` |
| **Aggregate Queries** | Faster GROUP BY operations | `SELECT region, COUNT(*) GROUP BY region` |
| **Range Queries** | Efficient range scans | `WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'` |
| **Join Queries** | Improved join performance when clustering on join keys | `JOIN ON table1.id = table2.id` |

<a name="58-clustering-example"></a>

## 5.8 Clustering Example
```sql
-- Clustered table (no partitioning)
CREATE TABLE dataset.orders (
  order_id INT64,
  customer_id INT64,
  region STRING,
  order_date DATE,
  amount NUMERIC
)
CLUSTER BY customer_id, region;

-- Partitioned AND clustered table (recommended)
CREATE TABLE dataset.orders (
  order_id INT64,
  customer_id INT64,
  region STRING,
  order_date DATE,
  amount NUMERIC
)
PARTITION BY DATE(order_date)
CLUSTER BY customer_id, region;
```

<a name="59-partitioning-vs-clustering"></a>

## 5.9 Partitioning vs Clustering

| Aspect | Partitioning | Clustering |
|--------|--------------|-----------|
| **Data Organization** | Divides table into separate partitions | Sorts data within partitions/table |
| **Cost Reduction** | Eliminates entire partitions from scan | Reduces blocks scanned within partitions |
| **Query Pattern** | Best for date/time range filters | Best for high-cardinality column filters |
| **DML Operations** | Cheaper when targeting specific partitions | Benefits all queries with clustered columns |
| **Column Limit** | 1 partition column | Up to 4 clustering columns |
| **Maintenance** | No automatic maintenance needed | Automatic reclustering in background |

<a name="510-when-to-use-clustering-over-partitioning"></a>

## 5.10 When to Use Clustering Over Partitioning

| Scenario | Reason |
|----------|--------|
| **Small partitions** | Partitioning results in < 1 GB per partition | 
| **Exceeds partition limit** | Would create > 4,000 partitions |
| **High mutation frequency** | Frequent updates/deletes affecting most partitions (every few minutes) |
| **High-cardinality filters** | Queries filter on columns with many distinct values (e.g., customer_id, product_sku) |
| **No time dimension** | Data doesn't have natural time-based partitioning column |

<a name="511-combining-partitioning-and-clustering"></a>

## 5.11 Combining Partitioning and Clustering

| Configuration | Use Case | Example |
|---------------|----------|---------|
| **Partition by date + Cluster by ID** | Time-based queries with entity filters | Partition: order_date, Cluster: customer_id, product_id |
| **Partition by date + Cluster by category** | Time-based queries with category filters | Partition: event_date, Cluster: country, device_type |
| **Partition by range + Cluster by attributes** | Range-based queries with attribute filters | Partition: customer_id (range), Cluster: region, status |

<a name="512-automatic-reclustering"></a>

## 5.12 Automatic Reclustering

| Aspect | Description |
|--------|-------------|
| **Trigger** | Activated when new data overlaps with existing key ranges |
| **Frequency** | Automatic, runs in background continuously |
| **Cost** | Free - no additional charges for reclustering |
| **Performance** | Maintains sort property and query performance over time |
| **Scope** | For partitioned tables, reclustering occurs within each partition |
| **Monitoring** | View reclustering history in `INFORMATION_SCHEMA.TABLE_STORAGE` |

<a name="513-clustering-column-order-best-practices"></a>

## 5.13 Clustering Column Order Best Practices

| Priority | Column Selection | Rationale |
|----------|------------------|-----------|
| **1st Column** | Most frequently filtered column with high cardinality | Maximum query pruning benefit |
| **2nd Column** | Second most frequently filtered or used in GROUP BY | Additional pruning within first column blocks |
| **3rd Column** | Often filtered in combination with first two | Further refines data locality |
| **4th Column** | Occasionally filtered or improves specific query patterns | Marginal additional benefit |

<a name="5131-example-column-order"></a>

### 5.13.1 Example Column Order
```sql
-- Good: Most selective filters first
CLUSTER BY customer_id, order_date, region, status

-- Poor: Less selective filters first
CLUSTER BY status, region, customer_id, order_date
```

<a name="514-performance-improvement-thresholds"></a>

## 5.14 Performance Improvement Thresholds

| Table Size | Partitioning Benefit | Clustering Benefit | Recommendation |
|------------|---------------------|-------------------|----------------|
| **< 1 GB** | Minimal | Minimal | Neither needed |
| **1 GB - 10 GB** | Moderate | Moderate | Clustering recommended |
| **10 GB - 100 GB** | High | High | Both recommended |
| **> 100 GB** | Very High | Very High | Both strongly recommended |

<a name="515-query-pruning-metrics"></a>

## 5.15 Query Pruning Metrics

| Metric | Description | Access |
|--------|-------------|--------|
| **Bytes Scanned** | Total data scanned by query | Query execution details |
| **Partitions Pruned** | Number of partitions skipped | Query execution plan |
| **Blocks Pruned** | Data blocks skipped due to clustering | Query execution plan |
| **Pruning Ratio** | Percentage of data eliminated | Calculate: (Total - Scanned) / Total |

<a name="516-common-patterns"></a>

## 5.16 Common Patterns

| Query Pattern | Optimal Configuration | Example |
|---------------|----------------------|---------|
| **Time-range + Entity** | Partition by date, Cluster by entity_id | `WHERE date BETWEEN ... AND customer_id = ...` |
| **Time-range + Category** | Partition by date, Cluster by category columns | `WHERE date >= ... AND region = ... AND product_type = ...` |
| **Entity + Status** | Cluster by entity_id, status | `WHERE user_id = ... AND status IN (...)` |
| **Geographic + Time** | Partition by date, Cluster by country, city | `WHERE date = ... AND country = ... AND city = ...` |

<a name="517-resource-reference"></a>

## 5.17 Resource Reference

| Topic | Documentation Link |
|-------|-------------------|
| **Partitioned Tables** | https://cloud.google.com/bigquery/docs/partitioned-tables |
| **Clustered Tables** | https://cloud.google.com/bigquery/docs/clustered-tables |
| **Creating Partitioned Tables** | https://cloud.google.com/bigquery/docs/creating-partitioned-tables |
| **Creating Clustered Tables** | https://cloud.google.com/bigquery/docs/creating-clustered-tables |

[↑ Back to Top](#top)
<a name="6-bigquery-best-practices"></a>

# 6. BigQuery Best Practices

<a name="61-cost-reduction"></a>

## 6.1 Cost Reduction

| Best Practice | Description | Impact | Example |
|---------------|-------------|--------|---------|
| **Avoid SELECT *** | Query only the columns you need | Reduces bytes scanned proportionally | `SELECT user_id, name` instead of `SELECT *` |
| **Price queries before running** | Use query validator to estimate cost | Prevents expensive mistakes | Check "Bytes to be processed" before execution |
| **Use partitioned tables** | Query only relevant date ranges | 50-90% cost reduction | `WHERE DATE(timestamp) = '2024-01-01'` |
| **Use clustered tables** | Reduce blocks scanned with clustering | 30-70% cost reduction | Cluster by frequently filtered columns |
| **Limit streaming inserts** | Batch load when possible; streaming costs $0.010 per 200 MB | Avoid streaming charges | Use batch loads for non-time-sensitive data |
| **Materialize query results in stages** | Break complex queries into intermediate tables | Reuse results, avoid re-processing | Create temp tables for multi-step transformations |
| **Use approximate aggregations** | Functions like APPROX_COUNT_DISTINCT | 10-30% faster, lower cost | `APPROX_COUNT_DISTINCT(user_id)` vs `COUNT(DISTINCT user_id)` |
| **Leverage query cache** | Cached results are free for 24 hours | 100% cost savings | Identical queries return cached results |
| **Set table expiration** | Auto-delete temporary tables | Reduces storage costs | Set expiration for staging tables |
| **Use long-term storage** | Tables unused for 90+ days get 50% discount | 50% storage cost reduction | Automatic after 90 days of no modifications |

<a name="62-query-performance---table-design"></a>

## 6.2 Query Performance - Table Design

| Best Practice | Description | Performance Gain | Example |
|---------------|-------------|------------------|---------|
| **Filter on partitioned columns** | Always filter partition column to prune partitions | Scans only relevant partitions | `WHERE DATE(order_date) BETWEEN '2024-01-01' AND '2024-01-31'` |
| **Denormalize data** | Reduce joins by combining related tables | Faster queries, fewer joins | Combine customer + address into single wide table |
| **Use nested/repeated columns** | Store related data in STRUCT and ARRAY types | Avoid joins, maintain relationships | `addresses ARRAY<STRUCT<street STRING, city STRING>>` |
| **Avoid oversharding** | Don't create too many small tables | Reduces metadata overhead | Use partitioned table instead of daily tables |
| **Optimize clustered columns** | Cluster by most frequently filtered columns first | Reduces blocks scanned | `CLUSTER BY customer_id, region, product_id` |

<a name="63-query-performance---external-data"></a>

## 6.3 Query Performance - External Data

| Best Practice | Description | When to Use | When to Avoid |
|---------------|-------------|-------------|---------------|
| **Use external data sources appropriately** | Query Cloud Storage, Bigtable, Cloud SQL directly | Infrequent queries, data exploration, federated analysis | High query performance requirements, frequent access |
| **Don't use for high performance** | External sources slower than native tables | Ad-hoc analysis only | Production dashboards, time-sensitive queries |
| **Load frequently accessed data** | Import external data into BigQuery tables | Data queried regularly | One-off analysis |

<a name="64-query-performance---query-optimization"></a>

## 6.4 Query Performance - Query Optimization

| Best Practice | Description | Performance Impact | Example |
|---------------|-------------|-------------------|---------|
| **Reduce data before JOIN** | Filter, aggregate, or limit data before joining | Faster joins, less data shuffling | Apply WHERE filters before JOIN clauses |
| **Avoid treating WITH as prepared statements** | WITH clauses are materialized each time | CTE executed multiple times if referenced multiple times | Use temp tables for reused results |
| **Avoid JavaScript UDFs** | JavaScript functions slower than native SQL | 10-100x slower | Use native SQL functions when possible |
| **Use approximate aggregation** | HyperLogLog++ for COUNT DISTINCT | Significantly faster for large datasets | `APPROX_COUNT_DISTINCT()` vs `COUNT(DISTINCT)` |
| **ORDER BY last** | Apply ORDER BY as final operation | Maximizes query performance | Filter → Aggregate → Join → ORDER BY |
| **Optimize join patterns** | Place largest table first, smallest last | Better data distribution | `large_table JOIN medium_table JOIN small_table` |

<a name="65-partitioning-and-clustering-best-practices"></a>

## 6.5 Partitioning and Clustering Best Practices

| Practice | Details | Benefit |
|----------|---------|---------|
| **Always filter partition column** | Include partition column in WHERE clause | Partition pruning reduces cost and time |
| **Use DATE functions correctly** | Use `DATE(timestamp_column)` for partition pruning | Ensures partition elimination |
| **Combine partitioning + clustering** | Partition by date, cluster by high-cardinality columns | Maximum query optimization |
| **Avoid small partitions** | Each partition should be > 1 GB | Reduces metadata overhead |
| **Don't exceed 4,000 partitions** | Stay within partition limits | Prevents table limitations |
| **Cluster by filter columns** | Order: most selective filter first | Maximum block pruning |

<a name="66-join-optimization"></a>

## 6.6 JOIN Optimization

| Best Practice | Description | Rationale |
|---------------|-------------|-----------|
| **Largest table first** | `FROM largest_table JOIN smaller_table` | Optimizes data distribution in joins |
| **Pre-filter before JOIN** | Apply WHERE clauses before JOIN | Reduces data volume in join operations |
| **Use appropriate JOIN type** | INNER, LEFT, CROSS - choose correctly | Avoid unnecessary data expansion |
| **Avoid CROSS JOIN** | Use only when truly needed | Can create massive result sets |
| **Join on clustering columns** | Cluster tables on join keys | Improves join locality |

<a name="661-join-order-example"></a>

### 6.6.1 JOIN Order Example
```sql
-- Best Practice: Largest to smallest
SELECT *
FROM large_table (10M rows)
JOIN medium_table (1M rows) ON large_table.id = medium_table.id
JOIN small_table (10K rows) ON medium_table.id = small_table.id;

-- Poor Practice: Random order
SELECT *
FROM small_table
JOIN large_table ON small_table.id = large_table.id
JOIN medium_table ON large_table.id = medium_table.id;
```

<a name="67-with-clause-best-practices"></a>

## 6.7 WITH Clause Best Practices

| Practice | Description | Recommendation |
|----------|-------------|----------------|
| **Don't treat as prepared statements** | WITH CTEs are not cached/prepared | Materialize as temp tables if used multiple times |
| **Single-use CTEs are fine** | CTE referenced once - no issue | Use for query organization |
| **Multiple-use CTEs** | CTE referenced 2+ times - create temp table | Avoids re-execution |

<a name="671-with-clause-example"></a>

### 6.7.1 WITH Clause Example
```sql
-- Poor: CTE referenced multiple times (executed twice)
WITH customer_stats AS (
  SELECT customer_id, COUNT(*) as order_count
  FROM orders
  GROUP BY customer_id
)
SELECT a.customer_id FROM customer_stats a
UNION ALL
SELECT b.customer_id FROM customer_stats b;

-- Better: Materialize as temp table
CREATE TEMP TABLE customer_stats AS
SELECT customer_id, COUNT(*) as order_count
FROM orders
GROUP BY customer_id;

SELECT customer_id FROM customer_stats
UNION ALL
SELECT customer_id FROM customer_stats;
```

<a name="68-nested-and-repeated-columns"></a>

## 6.8 Nested and Repeated Columns

| Practice | Description | Benefit | Example |
|----------|-------------|---------|---------|
| **Use STRUCT for related fields** | Group related columns together | Logical organization, no joins | `address STRUCT<street STRING, city STRING, zip STRING>` |
| **Use ARRAY for repeated data** | Store one-to-many relationships | Avoid JOIN with child tables | `orders ARRAY<STRUCT<order_id INT64, amount NUMERIC>>` |
| **Denormalize with ARRAY/STRUCT** | Combine parent-child into single row | Eliminates JOIN operations | Customer with array of orders |
| **Access with dot notation** | Query nested fields directly | Efficient nested data access | `SELECT address.city FROM table` |

<a name="681-nested-column-example"></a>

### 6.8.1 Nested Column Example
```sql
-- Traditional normalized (requires JOIN)
SELECT c.name, o.order_date
FROM customers c
JOIN orders o ON c.id = o.customer_id;

-- Denormalized with ARRAY (no JOIN)
SELECT name, order.order_date
FROM customers
CROSS JOIN UNNEST(orders) AS order;
```

<a name="69-approximate-aggregation-functions"></a>

## 6.9 Approximate Aggregation Functions

| Function | Use Case | Speed Improvement | Accuracy |
|----------|----------|-------------------|----------|
| **APPROX_COUNT_DISTINCT** | Count unique values | 10-30x faster | ~98% accurate |
| **APPROX_QUANTILES** | Calculate percentiles | 5-10x faster | Close approximation |
| **APPROX_TOP_COUNT** | Find most frequent items | Faster for large datasets | Statistical approximation |
| **APPROX_TOP_SUM** | Top items by sum | Faster aggregation | HyperLogLog++ algorithm |

<a name="610-javascript-udf-alternatives"></a>

## 6.10 JavaScript UDF Alternatives

| Task | Avoid JavaScript UDF | Use Instead |
|------|---------------------|-------------|
| **String manipulation** | JS string functions | Native `REGEXP_REPLACE`, `SUBSTR`, `CONCAT` |
| **Date calculations** | JS date logic | Native `DATE_ADD`, `DATE_DIFF`, `TIMESTAMP_TRUNC` |
| **Mathematical operations** | JS math functions | Native `ROUND`, `CEIL`, `FLOOR`, `POW` |
| **Conditional logic** | JS if/else | Native `CASE WHEN`, `IF`, `COALESCE` |
| **Array operations** | JS array methods | Native `ARRAY_AGG`, `UNNEST`, `ARRAY_CONCAT` |

<a name="611-anti-patterns-to-avoid"></a>

## 6.11 Anti-Patterns to Avoid

| Anti-Pattern | Why It's Bad | Better Approach |
|--------------|--------------|-----------------|
| **SELECT * in production** | Scans unnecessary columns, high cost | Select only needed columns |
| **No partition filter** | Scans entire table unnecessarily | Always filter on partition column |
| **Many small tables** | High metadata overhead (oversharding) | Use single partitioned table |
| **Frequent streaming for batch data** | Higher cost than batch loads | Use batch load jobs |
| **Complex JS UDFs in hot path** | Extremely slow performance | Use native SQL functions |
| **CROSS JOIN without limits** | Cartesian product explosion | Use proper JOIN conditions |
| **ORDER BY early** | Sorts unnecessarily large datasets | Apply ORDER BY at the end |
| **Repeated complex subqueries** | Re-executes same logic multiple times | Create temp tables or materialized views |

<a name="612-query-execution-order-for-performance"></a>

## 6.12 Query Execution Order for Performance

| Order | Operation | Rationale |
|-------|-----------|-----------|
| **1** | FROM clause | Start with data source |
| **2** | WHERE clause | Filter early to reduce data volume |
| **3** | JOIN clause | Join on reduced datasets |
| **4** | GROUP BY clause | Aggregate filtered data |
| **5** | HAVING clause | Filter aggregated results |
| **6** | SELECT clause | Project final columns |
| **7** | ORDER BY clause | Sort only final result set |
| **8** | LIMIT clause | Return only needed rows |

<a name="613-monitoring-and-optimization"></a>

## 6.13 Monitoring and Optimization

| Tool | Purpose | How to Use |
|------|---------|------------|
| **Query Validator** | Estimate bytes before execution | Check before running queries |
| **Execution Plan** | Understand query execution | Review explain plan for bottlenecks |
| **Query History** | Analyze past query performance | Identify slow/expensive queries |
| **INFORMATION_SCHEMA** | Query metadata and statistics | Analyze table and query patterns |
| **Slot Utilization** | Monitor compute usage | Optimize for flat-rate pricing |

[↑ Back to Top](#top)
<a name="7-ml-in-bigquery"></a>

# 7. ML in BigQuery

<a name="overview"></a>

## Overview

| Aspect | Description |
|--------|-------------|
| **BigQuery ML** | Create and execute machine learning models using SQL queries |
| **No Data Movement** | Train models directly in BigQuery without exporting data |
| **Supported Models** | Linear/Logistic Regression, K-Means, Time Series, Deep Neural Networks, Boosted Trees, AutoML, Imported TensorFlow/ONNX models |
| **Use Cases** | Prediction, classification, clustering, forecasting, recommendation systems |

<a name="bigquery-ml-workflow"></a>

## BigQuery ML Workflow

| Step | Function | Purpose |
|------|----------|---------|
| **1. Prepare Data** | SELECT columns and create ML table | Select relevant features and label |
| **2. Create Model** | `CREATE MODEL` with OPTIONS | Train ML model using SQL |
| **3. Evaluate Model** | `ML.EVALUATE()` | Assess model performance metrics |
| **4. Get Feature Info** | `ML.FEATURE_INFO()` | Understand feature transformations |
| **5. Make Predictions** | `ML.PREDICT()` | Generate predictions on new data |
| **6. Explain Predictions** | `ML.EXPLAIN_PREDICT()` | Understand feature importance |
| **7. Tune Hyperparameters** | OPTIONS with `hparam_range()` | Optimize model performance |
| **8. Deploy Model** | Export and serve with TensorFlow Serving | Productionize model for real-time inference |

<a name="step-1-data-preparation"></a>

## Step 1: Data Preparation

<a name="select-relevant-columns"></a>

### Select Relevant Columns
```sql
-- SELECT THE INTERESTED COLUMNS
SELECT 
  passenger_count, 
  trip_distance, 
  PULocationID, 
  DOLocationID, 
  payment_type, 
  fare_amount, 
  tolls_amount, 
  tip_amount
FROM `taxi-rides-ny.nytaxi.yellow_tripdata_partitioned` 
WHERE fare_amount != 0;
```

<a name="create-ml-training-table"></a>

### Create ML Training Table
```sql
-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT 
    passenger_count, 
    trip_distance, 
    CAST(PULocationID AS STRING), 
    CAST(DOLocationID AS STRING),
    CAST(payment_type AS STRING), 
    fare_amount, 
    tolls_amount, 
    tip_amount
  FROM `taxi-rides-ny.nytaxi.yellow_tripdata_partitioned` 
  WHERE fare_amount != 0
);
```

<a name="step-2-create-model"></a>

## Step 2: Create Model
```sql
-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `taxi-rides-ny.nytaxi.tip_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT'
) AS
SELECT *
FROM `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;
```

<a name="model-types-and-options"></a>

## Model Types and Options

| Model Type | Option Value | Use Case | Label Type |
|------------|--------------|----------|------------|
| **Linear Regression** | `linear_reg` | Predict continuous values | Numeric |
| **Logistic Regression** | `logistic_reg` | Binary classification | Binary (0/1) |
| **Multiclass Logistic** | `logistic_reg` | Multi-class classification | Categorical |
| **K-Means Clustering** | `kmeans` | Group similar records | Not required (unsupervised) |
| **ARIMA Plus** | `arima_plus` | Time series forecasting | Time series |
| **AutoML Tables** | `automl_classifier` / `automl_regressor` | Automated model selection | Binary/Multiclass/Numeric |
| **Boosted Trees** | `boosted_tree_classifier` / `boosted_tree_regressor` | High accuracy classification/regression | Binary/Multiclass/Numeric |
| **Deep Neural Network** | `dnn_classifier` / `dnn_regressor` | Complex patterns | Binary/Multiclass/Numeric |
| **Matrix Factorization** | `matrix_factorization` | Recommendation systems | Implicit/explicit ratings |
| **Imported Model** | `tensorflow` | Custom TensorFlow models | Various |

<a name="common-create-model-options"></a>

## Common CREATE MODEL Options

| Option | Description | Example Values |
|--------|-------------|----------------|
| **model_type** | Type of ML model | `linear_reg`, `logistic_reg`, `kmeans`, `arima_plus` |
| **input_label_cols** | Target column(s) to predict | `['tip_amount']`, `['is_churned']` |
| **DATA_SPLIT_METHOD** | How to split train/test data | `AUTO_SPLIT`, `RANDOM`, `SEQUENTIAL`, `NO_SPLIT`, `CUSTOM` |
| **data_split_eval_fraction** | Fraction for evaluation | `0.2` (20% for testing) |
| **l1_reg** | L1 regularization | `0.1`, `hparam_range(0, 20)` |
| **l2_reg** | L2 regularization | `0.1`, `hparam_candidates([0, 0.1, 1])` |
| **max_iterations** | Training iterations | `20` (default), `50` |
| **learn_rate** | Learning rate | `0.01`, `hparam_range(0.001, 0.1)` |
| **early_stop** | Stop training early if no improvement | `TRUE`, `FALSE` |
| **min_rel_progress** | Minimum relative loss improvement | `0.01` (1% improvement required) |

<a name="step-3-check-feature-information"></a>

## Step 3: Check Feature Information
```sql
-- CHECK FEATURES
SELECT * 
FROM ML.FEATURE_INFO(MODEL `taxi-rides-ny.nytaxi.tip_model`);
```

<a name="feature-info-output"></a>

### Feature Info Output

| Column | Description |
|--------|-------------|
| **input** | Original feature name |
| **feature_name** | Transformed feature name (after one-hot encoding, scaling, etc.) |
| **feature_type** | NUMERICAL, CATEGORICAL, etc. |
| **encoding** | Transformation applied (ONE_HOT_ENCODING, IDENTITY, etc.) |

<a name="step-4-evaluate-model"></a>

## Step 4: Evaluate Model
```sql
-- EVALUATE THE MODEL
SELECT *
FROM ML.EVALUATE(
  MODEL `taxi-rides-ny.nytaxi.tip_model`,
  (
    SELECT *
    FROM `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);
```

<a name="evaluation-metrics-by-model-type"></a>

### Evaluation Metrics by Model Type

| Model Type | Metrics | Description |
|------------|---------|-------------|
| **Linear Regression** | `mean_absolute_error`, `mean_squared_error`, `r2_score`, `median_absolute_error` | Lower MAE/MSE is better, R² closer to 1 is better |
| **Logistic Regression** | `precision`, `recall`, `accuracy`, `f1_score`, `log_loss`, `roc_auc` | Higher precision/recall/accuracy/F1/AUC is better, lower log loss is better |
| **K-Means Clustering** | `davies_bouldin_index`, `mean_squared_distance` | Lower values indicate better clustering |
| **Time Series (ARIMA)** | `MAE`, `RMSE`, `MAPE` | Lower values are better |

<a name="step-5-make-predictions"></a>

## Step 5: Make Predictions
```sql
-- PREDICT THE MODEL
SELECT *
FROM ML.PREDICT(
  MODEL `taxi-rides-ny.nytaxi.tip_model`,
  (
    SELECT *
    FROM `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);
```

<a name="prediction-output-columns"></a>

### Prediction Output Columns

| Column | Description |
|--------|-------------|
| **predicted_[label]** | Model's prediction for the label column |
| **Original columns** | All input feature columns from the query |

<a name="step-6-explain-predictions"></a>

## Step 6: Explain Predictions
```sql
-- PREDICT AND EXPLAIN
SELECT *
FROM ML.EXPLAIN_PREDICT(
  MODEL `taxi-rides-ny.nytaxi.tip_model`,
  (
    SELECT *
    FROM `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ), 
  STRUCT(3 AS top_k_features)
);
```

<a name="explanation-output"></a>

### Explanation Output

| Column | Description |
|--------|-------------|
| **predicted_[label]** | Model prediction |
| **top_feature_attributions** | Array of top features contributing to prediction |
| **attribution** | Feature importance score (positive = increases prediction, negative = decreases) |

<a name="step-7-hyperparameter-tuning"></a>

## Step 7: Hyperparameter Tuning
```sql
-- HYPER PARAM TUNING
CREATE OR REPLACE MODEL `taxi-rides-ny.nytaxi.tip_hyperparam_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])
) AS
SELECT *
FROM `taxi-rides-ny.nytaxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;
```

<a name="hyperparameter-tuning-functions"></a>

### Hyperparameter Tuning Functions

| Function | Description | Example |
|----------|-------------|---------|
| **hparam_range(min, max)** | Search continuous range | `l1_reg=hparam_range(0, 20)` |
| **hparam_candidates([values])** | Search discrete values | `l2_reg=hparam_candidates([0, 0.1, 1, 10])` |
| **num_trials** | Number of tuning trials | `num_trials=5` |
| **max_parallel_trials** | Parallel trial execution | `max_parallel_trials=2` |

<a name="view-tuning-results"></a>

### View Tuning Results
```sql
SELECT *
FROM ML.TRIAL_INFO(MODEL `taxi-rides-ny.nytaxi.tip_hyperparam_model`)
ORDER BY mean_squared_error ASC;
```

<a name="model-deployment"></a>

## Model Deployment

<a name="deployment-workflow"></a>

### Deployment Workflow

| Step | Command/Action | Purpose |
|------|----------------|---------|
| **1. Authenticate** | `gcloud auth login` | Authenticate with Google Cloud |
| **2. Export Model** | `bq --project_id taxi-rides-ny extract -m nytaxi.tip_model gs://taxi_ml_model/tip_model` | Export model to Cloud Storage |
| **3. Create Local Directory** | `mkdir /tmp/model` | Prepare local storage |
| **4. Download Model** | `gsutil cp -r gs://taxi_ml_model/tip_model /tmp/model` | Copy model locally |
| **5. Prepare Serving Directory** | `mkdir -p serving_dir/tip_model/1` | Create TensorFlow Serving structure |
| **6. Copy Model Files** | `cp -r /tmp/model/tip_model/* serving_dir/tip_model/1` | Move to serving directory |
| **7. Pull TF Serving Image** | `docker pull tensorflow/serving` | Get TensorFlow Serving Docker image |
| **8. Run TF Serving** | `docker run -p 8501:8501 --mount type=bind,source=$(pwd)/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &` | Start serving endpoint |
| **9. Test Prediction** | `curl -d '{"instances": [...]}' -X POST http://localhost:8501/v1/models/tip_model:predict` | Make prediction request |
| **10. Model Metadata** | `http://localhost:8501/v1/models/tip_model` | View model information |

<a name="deployment-commands"></a>

### Deployment Commands
```bash
<a name="step-1-authenticate"></a>

# Step 1: Authenticate
gcloud auth login

<a name="step-2-export-model-to-cloud-storage"></a>

# Step 2: Export model to Cloud Storage
bq --project_id taxi-rides-ny extract -m nytaxi.tip_model gs://taxi_ml_model/tip_model

<a name="step-3-6-prepare-local-serving-directory"></a>

# Step 3-6: Prepare local serving directory
mkdir /tmp/model
gsutil cp -r gs://taxi_ml_model/tip_model /tmp/model
mkdir -p serving_dir/tip_model/1
cp -r /tmp/model/tip_model/* serving_dir/tip_model/1

<a name="step-7-8-run-tensorflow-serving-with-docker"></a>

# Step 7-8: Run TensorFlow Serving with Docker
docker pull tensorflow/serving
docker run -p 8501:8501 \
  --mount type=bind,source=$(pwd)/serving_dir/tip_model,target=/models/tip_model \
  -e MODEL_NAME=tip_model \
  -t tensorflow/serving &

<a name="step-9-make-prediction-request"></a>

# Step 9: Make prediction request
curl -d '{
  "instances": [
    {
      "passenger_count": 1, 
      "trip_distance": 12.2, 
      "PULocationID": "193", 
      "DOLocationID": "264", 
      "payment_type": "2",
      "fare_amount": 20.4,
      "tolls_amount": 0.0
    }
  ]
}' -X POST http://localhost:8501/v1/models/tip_model:predict

<a name="step-10-view-model-metadata"></a>

# Step 10: View model metadata
curl http://localhost:8501/v1/models/tip_model
```

<a name="bigquery-ml-functions-reference"></a>

## BigQuery ML Functions Reference

| Function | Purpose | Example Usage |
|----------|---------|---------------|
| **ML.FEATURE_INFO()** | View feature transformations | `SELECT * FROM ML.FEATURE_INFO(MODEL my_model)` |
| **ML.EVALUATE()** | Evaluate model performance | `SELECT * FROM ML.EVALUATE(MODEL my_model, (SELECT * FROM test_data))` |
| **ML.PREDICT()** | Make predictions | `SELECT * FROM ML.PREDICT(MODEL my_model, (SELECT * FROM new_data))` |
| **ML.EXPLAIN_PREDICT()** | Predictions with feature importance | `SELECT * FROM ML.EXPLAIN_PREDICT(MODEL my_model, data, STRUCT(3 AS top_k_features))` |
| **ML.WEIGHTS()** | View model coefficients | `SELECT * FROM ML.WEIGHTS(MODEL my_model)` |
| **ML.TRAINING_INFO()** | View training metrics history | `SELECT * FROM ML.TRAINING_INFO(MODEL my_model)` |
| **ML.TRIAL_INFO()** | View hyperparameter tuning trials | `SELECT * FROM ML.TRIAL_INFO(MODEL my_model)` |
| **ML.CONFUSION_MATRIX()** | Classification confusion matrix | `SELECT * FROM ML.CONFUSION_MATRIX(MODEL my_model, (SELECT * FROM test_data))` |
| **ML.ROC_CURVE()** | ROC curve for binary classification | `SELECT * FROM ML.ROC_CURVE(MODEL my_model, (SELECT * FROM test_data))` |
| **ML.FEATURE_IMPORTANCE()** | Global feature importance | `SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL my_model)` |

<a name="data-split-methods"></a>

## Data Split Methods

| Method | Description | Use Case |
|--------|-------------|----------|
| **AUTO_SPLIT** | Automatic 80/20 train/test split | Default, most common |
| **RANDOM** | Random split with specified fraction | Custom split ratio |
| **SEQUENTIAL** | Split by sequence (time-based) | Time series data |
| **NO_SPLIT** | Use all data for training | When you have separate test table |
| **CUSTOM** | User-defined split column | Full control over train/test split |

<a name="custom-split-example"></a>

### Custom Split Example
```sql
-- Add split column to data
CREATE OR REPLACE TABLE `project.dataset.data_with_split` AS
SELECT 
  *,
  IF(RAND() < 0.8, 'TRAIN', 'TEST') AS split_col
FROM `project.dataset.original_data`;

-- Create model with custom split
CREATE OR REPLACE MODEL `project.dataset.model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['label'],
  DATA_SPLIT_METHOD='CUSTOM',
  data_split_col='split_col'
) AS
SELECT * FROM `project.dataset.data_with_split`;
```

<a name="best-practices"></a>

## Best Practices

| Practice | Description | Benefit |
|----------|-------------|---------|
| **Cast categorical features to STRING** | Convert numeric IDs to STRING for categorical treatment | Proper one-hot encoding |
| **Remove NULL labels** | Filter `WHERE label IS NOT NULL` | Avoid training errors |
| **Use AUTO_SPLIT for evaluation** | Let BigQuery handle train/test split | Prevent overfitting |
| **Start with default options** | Create baseline model first | Establish performance baseline |
| **Tune hyperparameters incrementally** | Adjust one parameter at a time | Understand impact of each parameter |
| **Check feature info** | Review transformations applied | Ensure correct feature engineering |
| **Monitor training info** | Track loss over iterations | Detect overfitting or convergence issues |
| **Use EXPLAIN_PREDICT for debugging** | Understand prediction drivers | Build trust in model |
| **Version models** | Include date/version in model name | Track model iterations |
| **Export models for production** | Use TensorFlow Serving or Vertex AI | Real-time predictions |

<a name="cost-considerations"></a>

## Cost Considerations

| Activity | Cost | Notes |
|----------|------|-------|
| **Model Training** | Charged as query (bytes processed) | CREATE MODEL costs based on data scanned |
| **Model Evaluation** | Charged as query | ML.EVALUATE counts as query |
| **Predictions** | Charged as query | ML.PREDICT costs based on data processed |
| **Model Storage** | Standard BigQuery storage rates | $0.02 per GB per month (active), $0.01 (long-term) |
| **Hyperparameter Tuning** | Multiple training runs | num_trials × training cost |