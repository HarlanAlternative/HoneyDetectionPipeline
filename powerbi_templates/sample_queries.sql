-- Sample SQL Queries for PowerBI Integration

-- 1. Basic Quality Data Query
SELECT 
    batch_id,
    sample_id,
    moisture,
    ph,
    diastase_activity,
    h_m_f,
    quality_score,
    quality_category,
    collection_date,
    lab_id,
    analyst
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY collection_date DESC;

-- 2. Quality Metrics Summary
SELECT 
    DATE(collection_date) as date,
    COUNT(*) as total_samples,
    AVG(quality_score) as avg_quality_score,
    COUNT(CASE WHEN quality_category = 'Excellent' THEN 1 END) as excellent_count,
    COUNT(CASE WHEN quality_category = 'Good' THEN 1 END) as good_count,
    COUNT(CASE WHEN quality_category = 'Fair' THEN 1 END) as fair_count,
    COUNT(CASE WHEN quality_category = 'Poor' THEN 1 END) as poor_count
FROM honey_quality_data
GROUP BY DATE(collection_date)
ORDER BY date DESC;

-- 3. Laboratory Performance Analysis
SELECT 
    lab_id,
    COUNT(*) as total_samples,
    AVG(quality_score) as avg_quality_score,
    COUNT(CASE WHEN quality_category = 'Excellent' THEN 1 END) as excellent_count,
    ROUND(
        COUNT(CASE WHEN quality_category = 'Excellent' THEN 1 END) * 100.0 / COUNT(*), 2
    ) as excellence_rate
FROM honey_quality_data
GROUP BY lab_id
ORDER BY excellence_rate DESC;

-- 4. Quality Trend Analysis
SELECT 
    DATE_TRUNC('week', collection_date) as week_start,
    COUNT(*) as samples_per_week,
    AVG(quality_score) as weekly_avg_score,
    AVG(moisture) as weekly_avg_moisture,
    AVG(ph) as weekly_avg_ph
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', collection_date)
ORDER BY week_start;

-- 5. Parameter Distribution Analysis
SELECT 
    'Moisture' as parameter,
    MIN(moisture) as min_value,
    MAX(moisture) as max_value,
    AVG(moisture) as avg_value,
    STDDEV(moisture) as std_dev
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
    'pH' as parameter,
    MIN(ph) as min_value,
    MAX(ph) as max_value,
    AVG(ph) as avg_value,
    STDDEV(ph) as std_dev
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
    'Diastase Activity' as parameter,
    MIN(diastase_activity) as min_value,
    MAX(diastase_activity) as max_value,
    AVG(diastase_activity) as avg_value,
    STDDEV(diastase_activity) as std_dev
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
    'HMF' as parameter,
    MIN(h_m_f) as min_value,
    MAX(h_m_f) as max_value,
    AVG(h_m_f) as avg_value,
    STDDEV(h_m_f) as std_dev
FROM honey_quality_data
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days';
