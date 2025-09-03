
# Sample SQL Queries

## Basic Queries
```sql
-- Quality by Lab
SELECT lab_id, AVG(quality_score) as avg_quality, COUNT(*) as samples
FROM honey_quality_data GROUP BY lab_id ORDER BY avg_quality DESC;

-- Quality Trends
SELECT DATE(collection_date) as date, AVG(quality_score) as avg_quality
FROM honey_quality_data GROUP BY DATE(collection_date) ORDER BY date;

-- Compliance Status
SELECT compliance_status, COUNT(*) as count
FROM honey_quality_data GROUP BY compliance_status;
```

## Export for PowerBI
```sql
SELECT * FROM honey_quality_data 
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY collection_date DESC;
```
        