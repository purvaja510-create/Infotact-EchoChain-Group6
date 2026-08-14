\# EchoChain Technology Stack



\## Overview



EchoChain uses a modern data engineering and analytics stack to combine

secondary-market data with internal manufacturing and warranty data.



\## Technologies



\### Scrapy

Used for building Python spiders to collect mock secondary-market

electronics listings, including pricing and condition information.



\### Databricks

Used as the primary data engineering and lakehouse environment for

processing and managing EchoChain data.



\### Delta Lake

Used as the storage layer within Databricks, following a

Bronze/Silver/Gold architecture.



\### PySpark

Used for large-scale data processing, including:



\- Data cleaning

\- Text processing

\- SKU extraction

\- Fuzzy matching

\- Data aggregation



\### Power BI

Used as the enterprise business intelligence and visualization layer

for executive reporting, lifecycle analysis, and drill-down dashboards.



\## Technology Flow



Scrapy

&#x20; ↓

Databricks / Delta Lake

&#x20; ↓

PySpark

&#x20; ↓

Silver / Gold Data

&#x20; ↓

Power BI

