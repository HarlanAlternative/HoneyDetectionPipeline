
# Sample SQL Queries for Honey Quality Analysis

## Basic Analysis Queries

### 1. Quality Summary by Laboratory
```sql
SELECT 
    lab_id,
    COUNT(*) as sample_count,
    AVG(quality_score) as avg_quality,
    MIN(quality_score) as min_quality,
    MAX(quality_score) as max_quality,
    STDDEV(quality_score) as quality_std
FROM honey_quality_data
GROUP BY lab_id
ORDER BY avg_quality DESC;
```

### 2. Quality Trends Over Time
```sql
SELECT 
    DATE(collection_date) as date,
    AVG(quality_score) as avg_quality,
    COUNT(*) as sample_count
FROM honey_quality_data
GROUP BY DATE(collection_date)
ORDER BY date;
```

### 3. Parameter Analysis
```sql
SELECT 
    quality_category,
    AVG(moisture) as avg_moisture,
    AVG(ph) as avg_ph,
    AVG(diastase_activity) as avg_diastase,
    AVG(h_m_f) as avg_hmf
FROM honey_quality_data
GROUP BY quality_category;
```

### 4. Compliance Analysis
```sql
SELECT 
    compliance_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM honey_quality_data), 2) as percentage
FROM honey_quality_data
GROUP BY compliance_status;
```

## Advanced Analysis Queries

### 5. Regional Performance
```sql
SELECT 
    region,
    lab_id,
    AVG(quality_score) as avg_quality,
    COUNT(*) as sample_count
FROM honey_quality_data
GROUP BY region, lab_id
ORDER BY region, avg_quality DESC;
```

### 6. Quality Score Distribution
```sql
SELECT 
    CASE 
        WHEN quality_score >= 95 THEN '95-100'
        WHEN quality_score >= 90 THEN '90-94'
        WHEN quality_score >= 80 THEN '80-89'
        WHEN quality_score >= 70 THEN '70-79'
        ELSE 'Below 70'
    END as score_range,
    COUNT(*) as count
FROM honey_quality_data
GROUP BY 
    CASE 
        WHEN quality_score >= 95 THEN '95-100'
        WHEN quality_score >= 90 THEN '90-94'
        WHEN quality_score >= 80 THEN '80-89'
        WHEN quality_score >= 70 THEN '70-79'
        ELSE 'Below 70'
    END
ORDER BY score_range DESC;
```

### 7. Parameter Correlation
```sql
SELECT 
    CORR(moisture, ph) as moisture_ph_correlation,
    CORR(moisture, quality_score) as moisture_quality_correlation,
    CORR(ph, quality_score) as ph_quality_correlation
FROM honey_quality_data;
```

## Data Export Queries

### 8. Export for PowerBI
```sql
SELECT 
    batch_id,
    sample_id,
    moisture,
    ph,
    diastase_activity,
    h_m_f,
    quality_score,
    quality_category,
    compliance_status,
    lab_id,
    analyst,
    region,
    collection_date
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY collection_date DESC;
```
        