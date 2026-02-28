# How to Use Marimo Notebook for Visualizations and SQL Queries

This guide explains how to explore your dlt pipeline data using Marimo notebooks with SQL queries. It is based on the `taxi_exploration.py` notebook in this project.

## Prerequisites

Install the required packages:

```bash
pip install marimo "ibis-framework[duckdb]" "marimo[sql]"
```

Or add to your `requirements.txt`:

```
dlt[duckdb]>=1.22.1
marimo
ibis-framework[duckdb]
marimo[sql]
```

**Important:** Run the pipeline at least once before exploring, so the DuckDB database and tables exist:

```bash
python taxi_pipeline.py
```

---

## Step 1: Launch Marimo

Start Marimo in edit mode:

```bash
marimo edit taxi_exploration.py
```

Marimo will print a URL (e.g. `http://localhost:2718?access_token=...`). Open it in your browser.

---

## Step 2: Connect to Your Pipeline Data

Create a cell that loads your dlt pipeline and obtains an Ibis connection to the dataset:

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
)

dataset = pipeline.dataset()
dataset_name = pipeline.dataset_name

# Use read_only=True to avoid lock conflicts with the pipeline
ibis_connection = dataset.ibis(read_only=True)
```

**Why `read_only=True`?** DuckDB uses file-based locking. If the pipeline or another process has the database open, you may see:

> Conflicting lock is held... Could not set lock on file

Using `read_only=True` lets Marimo read the data while other processes can still access it.

---

## Step 3: Run SQL Queries

Use `mo.sql()` to run SQL against your dataset. Pass the Ibis connection as the `engine`:

```python
import marimo as mo

_df = mo.sql(
    """
    SELECT * FROM trips LIMIT 10;
    """,
    engine=ibis_connection
)
```

**Table reference:** The `trips` table lives in your pipeline dataset. If you get "table not found", try qualifying it with the dataset name:

```sql
SELECT * FROM taxi_pipeline_dataset.trips LIMIT 10;
```

---

## Example Queries

### Date range of the dataset

```sql
SELECT 
    min(trip_pickup_date_time) AS start_date, 
    max(trip_dropoff_date_time) AS end_date
FROM trips;
```

### Proportion of trips paid with credit card

```sql
SELECT 100.0 * COUNT(CASE WHEN payment_type = 'Credit' THEN 1 END) / NULLIF(COUNT(*), 0) AS credit_card_pct
FROM trips;
```

**Note:** Use `100.0` (not `100`) so the result is a decimal. `NULLIF(COUNT(*), 0)` avoids division by zero when the table is empty.

### Total tips

```sql
SELECT sum(tip_amt) AS total_tips FROM trips;
```

### Trips by payment type

```sql
SELECT payment_type, COUNT(*) AS trip_count
FROM trips
GROUP BY payment_type
ORDER BY trip_count DESC;
```

### Average fare by passenger count

```sql
SELECT passenger_count, 
       COUNT(*) AS trips,
       ROUND(AVG(fare_amt), 2) AS avg_fare
FROM trips
GROUP BY passenger_count
ORDER BY passenger_count;
```

---

## Step 4: Add Visualizations

You can use the query result in Python and plot it. First, run a query that returns a result you can assign:

```python
# SQL cell - use a named output variable (no leading underscore)
payment_summary = mo.sql(
    """
    SELECT payment_type, COUNT(*) AS trip_count
    FROM trips
    GROUP BY payment_type;
    """,
    engine=ibis_connection
)
```

Then in another cell, create a chart:

```python
import plotly.express as px

# payment_summary is a DataFrame from the SQL result
fig = px.bar(
    payment_summary, 
    x="payment_type", 
    y="trip_count",
    title="Trips by Payment Type"
)
fig.show()
```

---

## Step 5: Use dlt Widgets (Optional)

To inspect pipeline loads and state, use the dlt package viewer:

```python
import marimo as mo
from dlt.helpers.marimo import render, load_package_viewer

# Must be awaited - use async cell
await render(load_package_viewer)
```

---

## SQL Tips

| Tip | Example |
|-----|---------|
| Use `100.0` for percentages | `100.0 * x / y` avoids integer division |
| Avoid division by zero | Use `NULLIF(COUNT(*), 0)` as denominator |
| Single scan for proportions | `COUNT(CASE WHEN condition THEN 1 END)` instead of two subqueries |
| Limit large results | Add `LIMIT 100` when exploring |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **Lock error on DuckDB** | Use `dataset.ibis(read_only=True)` |
| **Table not found** | Qualify with dataset: `taxi_pipeline_dataset.trips` |
| **Marimo not finding tables** | Ensure the pipeline has run and the `.duckdb` file exists |
| **SQL cell not available** | Install `marimo[sql]`: `pip install "marimo[sql]"` |

---

## File Structure

| File | Purpose |
|------|---------|
| `taxi_pipeline.py` | Runs the pipeline and loads data into DuckDB |
| `taxi_exploration.py` | Marimo notebook for exploration and SQL |
| `taxi_pipeline.duckdb` | DuckDB database (created after first pipeline run) |

---

## Further Reading

- [dlt: Explore data with marimo](https://dlthub.com/docs/general-usage/dataset-access/marimo)
- [marimo: Using SQL](https://docs.marimo.io/guides/working_with_data/sql)
- [marimo: Interactive dataframes](https://docs.marimo.io/guides/working_with_data/dataframes)
