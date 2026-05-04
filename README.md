# Honey Production Analytics Pipeline

Data-analytics project analysing real US honey production data (USDA NASS, 1998–2012),
backed by an industrial-grade ETL/BI pipeline ready to ingest laboratory quality data.

---

## Key Findings

> All numbers are reproducible from `notebooks/01_eda.ipynb`.

- **Production fell 35.8%** — US aggregate output dropped from **220 M lb (1998)** to **141 M lb (2012)** across 44 surveyed states; the decline accelerated after 2006, coinciding with Colony Collapse Disorder onset.
- **Price rose 184%** — Mean price per pound climbed from **$0.83 (1998)** to **$2.37 (2012)**; the correlation with year is r = **+0.694** (p < 0.0001), a textbook supply-side price signal.
- **Regional productivity gap is 33%** — One-way ANOVA (F = **16.8**, p < 0.0001) confirms the Midwest averages **67.9 lb/colony** vs the Northeast at **50.9 lb/colony**, driven by North Dakota's vast clover and canola forage.

---

## Dashboard

Interactive Streamlit dashboard — 4 pages, 14+ Plotly components.

```bash
streamlit run dashboard/app.py
```

| Page | Highlights |
|------|-----------|
| **Executive Overview** | 8 KPI cards · production area chart · top-10 states bar chart |
| **Quality Distribution** | PQI histogram · tier pie chart · state-year heatmap · regional box plots |
| **Parameter Deep Dive** | Pearson correlation matrix · scatter plots with OLS trendlines · IQR vs IsolationForest comparison |
| **Time Trends** | 4-panel annual trends · regional yield lines · price band chart with regression annotation |

See [`dashboard/README.md`](dashboard/README.md) for setup details and PQI methodology.

---

## Methodology

```
USDA NASS CSV (data/raw/)
        │
        ▼
notebooks/01_eda.ipynb   ←── EDA: overview · univariate · correlation ·
        │                        outlier detection · ANOVA · PQI validation
        ▼
dashboard/app.py          ←── Streamlit dashboard (4 pages, 14+ components)
        │
        ▼
ETL Pipeline (src/)       ←── Production infrastructure for lab chemistry data
        │                        (moisture, pH, HMF, diastase activity)
        ▼
PostgreSQL + Redis + Airflow + Prometheus + Grafana
```

### Dataset

| Field | Description |
|-------|-------------|
| `state` | US state (2-char FIPS abbreviation) |
| `numcol` | Number of honey-producing colonies |
| `yieldpercol` | Honey yield per colony (lb) |
| `totalprod` | Total production = numcol × yieldpercol (lb) |
| `stocks` | Stocks held at year-end (lb) |
| `priceperlb` | Average price per pound ($) |
| `prodvalue` | Total production value ($) |
| `year` | Survey year |

**Source**: USDA National Agricultural Statistics Service (NASS) Honey Production Survey.
Public domain (US federal government data). 626 observations · 44 states · 1998–2012 · 0 missing values.

Top 5 states by cumulative production (1998–2012):

| Rank | State | Production (M lb) |
|------|-------|-------------------|
| 1 | North Dakota | 475 |
| 2 | California | 348 |
| 3 | South Dakota | 266 |
| 4 | Florida | 247 |
| 5 | Montana | 157 |

### EDA Notebook (`notebooks/01_eda.ipynb`)

Six analysis sections with markdown commentary and **Key Finding** callouts:

1. Data overview — shape, dtypes, missing values, time range
2. Univariate analysis — histograms + boxplots for all numeric parameters
3. Correlation analysis — Pearson heatmap, top-3 correlated pairs
4. Outlier detection — IQR vs IsolationForest comparison (116 vs 32 flagged; 32 agree)
5. Regional group comparison — ANOVA across Census regions, p-values reported
6. Production Quality Index — composite PQI, distribution validation, year-over-year trends

### Production Quality Index (PQI)

The ETL pipeline's `_calculate_quality_score()` requires laboratory physicochemical measurements
(moisture, pH, HMF, diastase activity) not present in the public USDA survey.
PQI is the analytically appropriate substitute for public-data analysis:

```
pqi_raw   = (z_score(yieldpercol) + z_score(priceperlb)) / 2
pqi_score = rescaled to [0, 100]
```

PQI rose from a national mean of **32.7 (1998)** to **53.0 (2012)**.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Analysis | Python 3.8+, pandas, NumPy, SciPy, scikit-learn |
| Visualisation | Plotly, Streamlit, Matplotlib, Seaborn |
| Notebook | Jupyter (nbformat/nbconvert) |
| ETL | Apache Spark 3.5, pandas, Great Expectations, Pandera |
| Storage | PostgreSQL 15, Redis 7 |
| Orchestration | Apache Airflow 2.8 |
| Monitoring | Prometheus + Grafana |
| Deployment | Docker Compose |

---

## Architecture

The pipeline is designed for two tiers of data:

**Tier 1 — Public production data** (this project's analysis):
USDA NASS CSV → EDA notebook → Streamlit dashboard

**Tier 2 — Lab chemistry data** (ETL pipeline infrastructure):
Lab CSV/Excel (moisture, pH, HMF, diastase) → `IndustrialETLProcessor` →
PostgreSQL + Redis → PowerBI templates → Prometheus monitoring

```
HoneyDetectionPipeline/
├── data/raw/                    # Raw data (USDA NASS CSV)
├── notebooks/01_eda.ipynb       # Executed EDA with outputs
├── dashboard/app.py             # Streamlit interactive dashboard
├── src/
│   ├── industrial_pipeline.py   # ETL orchestrator
│   ├── industrial_etl.py        # Spark + quality scoring
│   ├── unified_powerbi_generator.py  # Excel/CSV/PowerBI exports
│   ├── powerbi_integration.py   # DAX measures, data model
│   └── monitoring_system.py     # Prometheus metrics
├── config/etl_config.json       # Thresholds & connections
├── docker-compose.production.yml
└── requirements.txt
```

### Quality scoring algorithm (for lab chemistry data)

Scores samples 0–100 based on deductions from four physicochemical parameters:

| Parameter | Target | Accepted range |
|-----------|--------|----------------|
| Moisture | 17.5% | 15–20% |
| pH | 5.0 | 3.5–6.5 |
| Diastase activity | ≥ 8.0 DN | ≥ 8.0 |
| HMF | ≤ 40 mg/kg | ≤ 40 |

Categories: **Premium** (≥95) · **Excellent** (90–94) · **Good** (80–89) · **Fair** (70–79) · **Poor** (<70)

### Docker deployment

```bash
docker-compose -f docker-compose.production.yml up -d  # PostgreSQL, Redis, Airflow, Prometheus, Grafana
```

### Run ETL pipeline

```bash
./run_pipeline.sh --demo      # generate fixtures + ETL + PowerBI templates
./run_pipeline.sh --status    # system status
```
