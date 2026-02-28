import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    #%% cell 1
    import dlt

    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
    )

    # Use read_only=True for exploration - allows concurrent access
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name

    # Get Ibis connection in read-only mode
    ibis_connection = dataset.ibis(read_only=True)
    return (ibis_connection,)


@app.cell
def _():
    import marimo as mo
    from dlt.helpers.marimo import render, load_package_viewer


    return load_package_viewer, mo, render


@app.cell
def _(ibis_connection, mo, trips):
    _df = mo.sql(
        f"""
        SELECT * FROM trips LIMIT 10;
        """,
        engine=ibis_connection
    )
    return


@app.cell
async def _(load_package_viewer, render):
    #%% cell
    await render(load_package_viewer)
    return


@app.cell
def _(ibis_connection, mo, trips):
    _df = mo.sql(
        f"""
        -- Question 1: What is the start date and end date of the dataset?
        SELECT min(trip_pickup_date_time) as start_date, 
            max(trip_dropoff_date_time) as end_date
            FROM trips;
        """,
        engine=ibis_connection
    )
    return


@app.cell
def _(ibis_connection, mo, trips):
    _df = mo.sql(
        f"""
        -- Question 2: What proportion of trips are paid with credit card?
        SELECT 100.0 * COUNT(CASE WHEN payment_type = 'Credit' THEN 1 END) / NULLIF(COUNT(*), 0) AS credit_card_pct
        FROM trips;
        """,
        engine=ibis_connection
    )
    return


@app.cell
def _(ibis_connection, mo, trips):
    _df = mo.sql(
        f"""
        --Question 3: What is the total amount of money generated in tips?
        SELECT sum(tip_amt) FROM trips;
        """,
        engine=ibis_connection
    )
    return


if __name__ == "__main__":
    app.run()
