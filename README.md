# **Lead Data Processing - DBT Pipeline**

## **Project Overview**
This project builds a DBT pipeline to integrate and transform multiple lead sources into a standardized data model. The objective is to create a reusable structure that allows for deduplication, validation, and enrichment of Salesforce lead data.

### **Sources & Modeling Approach**
- **Raw Sources**:
  - `sf_leads` (Salesforce lead data)
  - `source1`, `source2`, `source3` (monthly inbound files)
- **Staging Models (`stg_*`)**:
  - Standardize column names, data types, and formats.
  - Handle missing values and basic deduplication.
- **Intermediate Models (`int_*`)**:
  - Identify existing vs. new leads.
  - Implement initial deduplication based on phone number.
  - Normalize key data points (phone numbers, addresses).
- **Final Model (`fct_leads`)**:
  - One row per lead, combining all sources.
  - Identify lead status and source.
  - Provide a unified dataset for outreach and analytics.

---

## **Tradeoffs & Future Enhancements**
Due to time constraints, several optimizations were left out:
- **More Robust Deduplication**: A full fuzzy-matching strategy for deduping leads beyond exact matches.
- **Automated Schema Handling**: Dynamic ingestion to adapt to schema changes in source files.
- **Historical Change Tracking**: Capturing field changes over time.
- **Additional QA Testing**: Ensuring data consistency across all transformations.

---

## **Long-Term ETL Considerations**
- **Scalability**: Files are 100GB+; need efficient partitioning, incremental processing, and optimized Snowflake table structures. I would try to push this level of processing into S3, Glue, and Athena.
- **Schema Drift Management**: Implement automated schema validation with alerts for changes.
- **Data Validation & QA**: Enforce uniqueness, null checks, and referential integrity in DBT tests.
- **Lead Enrichment**: Incorporate third-party sources to enhance leads.
- **Performance Optimization**: Use Snowflake materialized views and incremental models to minimize recomputation.

---

## **Testing & Validation**
- **DBT Tests**: Unique constraints, null checks, duplicate detection.
- **Row Count Comparisons**: Between raw, staged, and final datasets.
- **Manual Spot Checks**: Automate the reviewing of sample records for expected transformations.

---

## **Next Steps**
- Implement incremental refresh strategies to handle full monthly reloads.
- Build automated change tracking for Salesforce integration.
- Enhance deduplication with probabilistic matching techniques.