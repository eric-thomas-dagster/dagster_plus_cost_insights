# Cost Insights Extension Guide

This repository contains resources and examples for extending Dagster Cloud's cost insights functionality to support additional data sources.

## Quick Answer

**Yes, you can add more cost insights integrations!** The architecture is extensible, though you may need to work around some hardcoded source checks.

## What's Included

### Documentation
- **`ANSWERS.md`** - Direct answers to your questions about extensibility
- **`EXTENDING_COST_INSIGHTS.md`** - Detailed guide on how to extend cost insights
- **`CHECKLIST.md`** - Checklist for examining the reference code

### Example Implementations
- **`databricks_insights_example.py`** - Example Databricks cost insights integration
- **`redshift_insights_example.py`** - Example Redshift cost insights integration

### Tools
- **`examine_reference.py`** - Script to analyze the reference repository for hardcoding

## Getting Started

### 1. Clone the Reference Repository

```bash
git clone https://github.com/dagster-io/dagster-cloud.git
```

### 2. Examine the Code Structure

```bash
python examine_reference.py /path/to/dagster-cloud
```

This will identify:
- Hardcoded source checks
- Source-specific code locations
- Insights asset definitions
- Resource definitions

### 3. Study the Patterns

Review the Snowflake and BigQuery implementations in:
```
dagster-cloud/dagster_cloud/dagster_insights/
```

### 4. Implement New Sources

Use the example files as templates:
- Follow the same resource pattern
- Follow the same asset pattern
- Use the same metrics emission mechanism

## Supported Sources (To Add)

- **Databricks** ⭐ (especially important)
- **Amazon Redshift**
- **Azure Synapse Analytics / Azure SQL Data Warehouse**
- **AWS S3/Glacier**
- **Google Cloud Storage (GCS)**
- **Azure Data Lake Storage**

## Key Questions Answered

### Is it possible?
Yes! The architecture is designed to be extensible.

### Is there hardcoding?
Possibly, but it's likely minimal. The examination script will help identify it.

### What needs to be done?
1. Create resource classes for each new source
2. Create insights assets for each new source
3. Register them (may need to extend registration if hardcoded)
4. Implement query/operation matching logic

## Next Steps

1. Read `ANSWERS.md` for direct answers to your questions
2. Read `EXTENDING_COST_INSIGHTS.md` for implementation details
3. Use `CHECKLIST.md` when examining the reference code
4. Run `examine_reference.py` to identify hardcoding
5. Use the example files as templates for your implementations

## Resources

- [Dagster Insights Documentation](https://docs.dagster.io/dagster-plus/features/insights)
- [Reference Repository](https://github.com/dagster-io/dagster-cloud/tree/main/dagster-cloud/dagster_cloud/dagster_insights)


