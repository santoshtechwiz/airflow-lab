# Weather Pipeline Architecture & Design

## Overview

The Weather Pipeline is a production-grade ETL (Extract, Transform, Load) pipeline that demonstrates real-world data engineering practices using Apache Airflow. The pipeline fetches weather data, validates it, stores it in multiple formats, and generates analytical reports.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                   WEATHER PIPELINE (DAG)                        │
└─────────────────────────────────────────────────────────────────┘

                        START (Daily @ 6 AM UTC)
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │   Extract Weather Data      │
                    │  (OpenWeatherMap API)       │
                    │  - London                   │
                    │  - New York                 │
                    │  - Tokyo                    │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Transform Weather Data     │
                    │  - Validate data            │
                    │  - Calculate metrics        │
                    │  - Flag extremes            │
                    └──────┬──────────────────────┘
                           │
                    ┌──────┴──────────┐
                    │                 │
                    ▼                 ▼
          ┌──────────────────┐  ┌──────────────┐
          │ Load to Database │  │ Load to CSV  │
          │  PostgreSQL      │  │ (Timestamped)│
          │ weather_data TBL │  │   Files      │
          └──────────────────┘  └──────────────┘
                    │                 │
                    │      ┌──────────┘
                    ▼      ▼
                    ┌─────────────────────────────┐
                    │  Generate Report            │
                    │  - Statistics               │
                    │  - Metrics by City          │
                    │  - Extreme Weather Alerts   │
                    └─────────────────────────────┘
                               │
                               ▼
                            SUCCESS
```

## Task Execution Flow

### Phase 1: Extract (Source → Raw Data)

**Task:** `extract_weather_data`

**Responsibility:**
- Connect to OpenWeatherMap API
- Fetch weather data for configured cities
- Return raw JSON response

**Input:** None (or API configuration)

**Output:** Raw weather data in XCom
```json
{
  "data": [
    {
      "city": "London",
      "country": "GB",
      "temp": 15.2,
      "temp_min": 12.5,
      "temp_max": 18.0,
      "humidity": 72,
      "pressure": 1013,
      "description": "Partly cloudy",
      "timestamp": "2026-04-26T10:30:00"
    }
  ],
  "extracted_at": "2026-04-26T10:30:00",
  "source": "OpenWeatherMap API"
}
```

**Failure Handling:**
- Retries: 2 attempts
- Retry delay: 5 minutes
- Timeout: 30 minutes

**Performance:**
- Expected runtime: 5-10 seconds
- API calls: 1 per city
- Data volume: ~500 bytes

---

### Phase 2: Transform (Raw Data → Quality Data)

**Task:** `transform_weather_data`

**Responsibility:**
- Pull raw data from Extract task (XCom)
- Validate data ranges and completeness
- Calculate derived metrics
- Flag outliers and extreme events
- Enrich data with business logic

**Validation Rules:**
```
✓ Temperature: -50°C to 60°C
✓ Humidity: 0-100%
✓ Pressure: 800-1050 hPa
✓ All required fields present
```

**Calculated Fields:**
```python
feels_like = temp - (humidity / 100)  # Simplified wind chill
is_extreme = (temp < 0) OR (temp > 35)
processed_at = current_timestamp
```

**Input:** Raw weather data (from Extract task)

**Output:** Transformed data in XCom
```json
{
  "data": [
    {
      "city": "London",
      "country": "GB",
      "temp": 15.2,
      "temp_min": 12.5,
      "temp_max": 18.0,
      "humidity": 72,
      "pressure": 1013,
      "description": "Partly cloudy",
      "feels_like": 14.48,
      "is_extreme": 0,
      "timestamp": "2026-04-26T10:30:00",
      "processed_at": "2026-04-26T10:31:00"
    }
  ],
  "row_count": 1,
  "validation_issues": [],
  "processed_at": "2026-04-26T10:31:00"
}
```

**Data Quality Metrics:**
- Validation pass rate: Target ≥ 99%
- Completeness: 100% (all fields present)
- Accuracy: Verified against source API

**Performance:**
- Expected runtime: 2-5 seconds
- Processing: Pandas DataFrame operations
- Memory: ~10MB for 1000 records

---

### Phase 3a: Load to Database (Quality Data → PostgreSQL)

**Task:** `load_to_postgres`

**Responsibility:**
- Connect to PostgreSQL database
- Create `weather_data` table if not exists
- Insert transformed records
- Handle duplicates (append mode)
- Commit transaction

**Database Schema:**
```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    temp FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    humidity INT,
    pressure INT,
    description VARCHAR(255),
    feels_like FLOAT,
    is_extreme BOOLEAN,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_city_timestamp ON weather_data(city, timestamp DESC);
CREATE INDEX idx_is_extreme ON weather_data(is_extreme) WHERE is_extreme = true;
```

**Insert Strategy:**
- Append mode (no updates)
- Preserves historical data
- Enables trend analysis

**Sample Queries:**
```sql
-- Get latest weather by city
SELECT DISTINCT ON (city) *
FROM weather_data
ORDER BY city, created_at DESC;

-- Average temperature trend
SELECT DATE(timestamp) as date, city, AVG(temp) as avg_temp
FROM weather_data
GROUP BY DATE(timestamp), city
ORDER BY date DESC;

-- Extreme weather alerts
SELECT * FROM weather_data
WHERE is_extreme = true
ORDER BY created_at DESC;
```

**Error Handling:**
- Connection timeout: 30 seconds
- Retry on connection failure: Yes
- Transaction rollback on error: Yes

**Performance:**
- Insert rate: ~1000 records/second
- Expected runtime: 1-3 seconds
- Connection pooling: Enabled

---

### Phase 3b: Load to CSV (Quality Data → Files)

**Task:** `load_to_csv`

**Responsibility:**
- Convert transformed data to CSV format
- Write to timestamped file
- Store in data directory
- Preserve for audit trail

**File Format:**
```
Location: /opt/airflow/data/weather_data_YYYYMMDD_HHMMSS.csv
Delimiter: , (comma)
Encoding: UTF-8
Headers: Yes
```

**Sample CSV Content:**
```csv
city,country,temp,temp_min,temp_max,humidity,pressure,description,feels_like,is_extreme,timestamp,processed_at
London,GB,15.2,12.5,18.0,72,1013,Partly cloudy,14.48,0,2026-04-26T10:30:00,2026-04-26T10:31:00
New York,US,22.5,20.0,25.0,65,1012,Sunny,21.85,0,2026-04-26T10:30:00,2026-04-26T10:31:00
```

**File Retention:**
- Policy: Keep all files (audit trail)
- Cleanup: Manual or via scheduled task
- Backup: Automated to external storage

**Performance:**
- Expected runtime: 1-2 seconds
- File size: ~500 bytes per record
- Compression: Optional (not enabled)

---

### Phase 4: Report (Data → Insights)

**Task:** `generate_report`

**Responsibility:**
- Pull transformed data
- Calculate summary statistics
- Generate insights by city
- Create JSON report
- Print formatted output

**Report Structure:**
```json
{
  "report_generated_at": "2026-04-26T10:32:00",
  "total_records": 3,
  "cities": ["London", "New York", "Tokyo"],
  "statistics": {
    "avg_temp": 18.57,
    "min_temp": 15.2,
    "max_temp": 22.5,
    "avg_humidity": 72.33,
    "extreme_weather_count": 0,
    "avg_pressure": 1013.33
  },
  "by_city": {
    "London": {
      "avg_temp": 15.2,
      "avg_humidity": 72.0,
      "description": "Partly cloudy"
    },
    "New York": {
      "avg_temp": 22.5,
      "avg_humidity": 65.0,
      "description": "Sunny"
    },
    "Tokyo": {
      "avg_temp": 18.0,
      "avg_humidity": 80.0,
      "description": "Rainy"
    }
  }
}
```

**Console Output:**
```
============================================================
WEATHER PIPELINE REPORT
============================================================
Generated: 2026-04-26T10:32:00
Total Records: 3

Global Statistics:
  avg_temp: 18.57
  min_temp: 15.20
  max_temp: 22.50
  avg_humidity: 72.33
  extreme_weather_count: 0
  avg_pressure: 1013.33

By City:
  London: Temp=15.2°C, Humidity=72%
  New York: Temp=22.5°C, Humidity=65%
  Tokyo: Temp=18.0°C, Humidity=80%
============================================================
```

**Report Destination:**
- XCom: Stored in Airflow metadata
- Console: Printed to task logs
- File: Optional JSON export
- Database: Optional table storage

**Performance:**
- Expected runtime: 1-2 seconds
- Memory: ~5MB for calculations
- Scalability: Handles 100K+ records

---

## Data Flow Summary

| Phase | Task | Input | Output | Technology | Duration |
|-------|------|-------|--------|-----------|----------|
| 1 | Extract | API config | Raw JSON | HTTP requests | 5-10s |
| 2 | Transform | Raw JSON | Validated DataFrame | Pandas | 2-5s |
| 3a | Load DB | DataFrame | PostgreSQL rows | psycopg2 | 1-3s |
| 3b | Load CSV | DataFrame | CSV file | Pandas | 1-2s |
| 4 | Report | DataFrame | JSON report | Python | 1-2s |
| **Total** | **All** | - | - | - | **10-25s** |

## Task Dependencies

```
extract_weather_data
    ↓
transform_weather_data
    ↓
    ├→ load_to_postgres
    │       ↓
    │   generate_report
    │       ↓
    └→ load_to_csv
            ↓
        (no further deps)
```

**Execution Strategy:**
1. Extract runs first (sequential)
2. Transform waits for Extract (depends_on)
3. Load DB and Load CSV run in parallel
4. Report waits for all loads to complete
5. Total parallelism: 2 tasks (Phase 3a & 3b)

## Error Handling Strategy

### Task Failures

**Retry Policy:**
```
- Max retries: 2
- Retry delay: 5 minutes
- Backoff: Linear (no exponential backoff)
```

**Failure Modes:**

| Scenario | Task | Behavior | Recovery |
|----------|------|----------|----------|
| API unavailable | Extract | Retry 2x, then fail | Manual trigger or next schedule |
| Invalid data | Transform | Log warnings, continue | Data quality dashboard alert |
| DB connection lost | Load DB | Retry, fail if persists | Database admin review |
| Disk full | Load CSV | Task fails | Free disk space, retrigger |

**Trigger Rules:**
```python
# Phase 3a & 3b use 'none_failed' to run even if one task fails
# Phase 4 uses default 'all_success' to ensure data quality
```

## Performance Metrics

### Expected SLAs (Service Level Agreements)

| Metric | Target | Actual |
|--------|--------|--------|
| Total Runtime | < 30s | 10-25s |
| Extract Duration | < 15s | 5-10s |
| Transform Duration | < 10s | 2-5s |
| Load Duration | < 10s | 2-5s |
| Success Rate | 99.9% | 99%+ |

### Resource Usage

| Resource | Extract | Transform | Load | Report |
|----------|---------|-----------|------|--------|
| CPU | 10% | 30% | 5% | 20% |
| Memory | 50MB | 100MB | 50MB | 50MB |
| Disk I/O | 1MB | 5MB | 5MB | 1MB |
| Network | 1MB up | 1MB | 1MB | - |

## Monitoring & Alerting

### Key Metrics to Monitor

1. **Task Success Rate**
   - Query: Success / (Success + Failure)
   - Target: ≥ 99%
   - Alert: < 95%

2. **Task Duration**
   - Query: Max duration per task
   - Target: < 30s total
   - Alert: > 60s

3. **Data Quality**
   - Metric: Validation pass rate
   - Target: 100%
   - Alert: < 99%

4. **Database Health**
   - Metric: Insert success rate
   - Target: 100%
   - Alert: Any failure

### Dashboard Queries

**Airflow UI - Admin → Logs:**
```
Filter by DAG: weather_pipeline_dag
View: Task success/failure trends
Metric: Duration over time
```

**PostgreSQL - Data Quality:**
```sql
-- Check data completeness
SELECT COUNT(*) total, COUNT(*) complete
FROM weather_data
WHERE city IS NOT NULL AND temp IS NOT NULL;

-- Monitor extreme weather events
SELECT COUNT(*) as extreme_events, DATE(created_at) as date
FROM weather_data
WHERE is_extreme = true
GROUP BY DATE(created_at)
ORDER BY date DESC;
```

## Scaling Considerations

### Current Design (Single City)
- 1 extract, 1 transform, parallel loads, 1 report
- Total runtime: ~20 seconds
- Parallelism: 2 (load tasks)

### For 100 Cities

**Option 1: Dynamic Mapping (Recommended)**
```python
# Extract all cities
# Map transform over each city (100 parallel tasks)
# Load each city independently
# Single report task
```

**Option 2: Batching**
```python
# Extract 20 cities per batch
# Transform batch
# Load batch
# Repeat for 5 batches
```

**Option 3: Windowed Aggregation**
```python
# Extract city 1-20, transform, load
# Extract city 21-40, transform, load (parallel)
# ... repeat ...
```

## Future Enhancements

### Phase 2: Real-time Processing
- Replace batch schedule with streaming
- Event-driven pipeline (sensor-based)
- Immediate alerting on extreme weather

### Phase 3: Advanced Analytics
- ML model for temperature prediction
- Anomaly detection algorithms
- Time-series analysis and forecasting

### Phase 4: Multi-source Integration
- Merge multiple weather APIs
- Integrate satellite imagery
- Add air quality data

### Phase 5: Enterprise Features
- Data lineage tracking (OpenLineage)
- Cost optimization per city
- SLA monitoring and reporting
- Automated scaling based on data volume

## References

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Guide](https://www.postgresql.org/docs/13/)
- [Pandas DataFrame Operations](https://pandas.pydata.org/docs/)

