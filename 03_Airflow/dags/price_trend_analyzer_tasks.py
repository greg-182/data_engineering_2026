"""STUB -- BTC price analyzer decomposed into one task per step.

Same pipeline as price_trend_analyzer.py, but split so the DAG graph shows the
actual data flow:

    fetch_price -> store_price -> compute_rolling_average -> decide_order -> record_order

Every step is currently an EmptyOperator: a placeholder that does nothing and
always succeeds. Trigger the DAG as-is and it goes green end to end -- that is
the point. You get the shape of the pipeline first, then replace the
placeholders with real work one at a time, and the graph never breaks while you
are half-finished.

Your job: replace each EmptyOperator with a PythonOperator calling the matching
function below. Work left to right and re-run after each swap.

Steps hand values to each other through XCom. Returning a value from a callable
pushes it; read it back with ti.xcom_pull(task_ids="<upstream task id>").
XCom payloads live in the metadata database, so keep them small -- a price and a
timestamp, never a dataset.

Run the whole chain with:
    docker compose exec airflow-scheduler \
      airflow dags test price_trend_analyzer_tasks

Or from the UI.

Testing a single task in isolation only works for fetch_price; the others need
XCom values from upstream, which a lone `airflow tasks test` will not have.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator  # noqa: F401 -- you will need this
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os
import pendulum
import requests
from datetime import timedelta

DATA_DIR = "/tmp/data/orders"
PRICE_API = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
CONN_ID = "prices_db"
ROLLING_WINDOW_MINUTES = 15

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def fetch_price(**context):
    """Step 1 -- call the price API. The only step that touches the network.

    TODO: GET PRICE_API, raise on a bad HTTP status, read the "price" field,
    and return {"price": ..., "ts": context["logical_date"].isoformat()}.

    Return the timestamp as a string: XCom serializes values to JSON, and a
    datetime does not survive that round trip cleanly.
    """
    response = requests.get(PRICE_API, timeout=10)
    response.raise_for_status()
    price = float(response.json()["price"])
    ts = context["logical_date"]
    print(f"Fetched BTC price {price} for {ts.isoformat()}")
    # Returning a value pushes it to XCom under the key "return_value".
    return {"price": price, "ts": ts.isoformat()} 


def store_price(**context):
    """Step 2 -- insert the fetched price into btc_prices.

    TODO: xcom_pull the payload from "fetch_price", then INSERT (ts, price).
    """
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="fetch_price")
    price, ts = payload["price"], payload["ts"]

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    pg.run(
        "INSERT INTO btc_prices (ts, price) VALUES (%s, %s)",
        parameters=(ts, price),
    )
    print(f"Stored price {price} at {ts}") 


def compute_rolling_average(**context):
    """Step 3 -- average the last ROLLING_WINDOW_MINUTES of prices.

    TODO: read the timestamp from fetch_price's XCom (pendulum.parse turns the
    string back into a datetime), SELECT AVG(price) over the window, INSERT the
    result into btc_rolling_avg, and return it for the next task.

    Bound the window at both ends (>= cutoff AND <= ts) so re-running an old
    interval cannot pull in rows that are newer than the run itself.
    """
    ti = context["ti"]
    ts = pendulum.parse(ti.xcom_pull(task_ids="fetch_price")["ts"])
    cutoff = ts - timedelta(minutes=ROLLING_WINDOW_MINUTES)

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    row = pg.get_first(
        "SELECT AVG(price) FROM btc_prices WHERE ts >= %s AND ts <= %s",
        parameters=(cutoff, ts),
    )
    rolling_avg = float(row[0])

    pg.run(
        "INSERT INTO btc_rolling_avg (ts, rolling_avg) VALUES (%s, %s)",
        parameters=(ts, rolling_avg),
    )
    print(f"Rolling average over {ROLLING_WINDOW_MINUTES} min: {rolling_avg}")
    return rolling_avg


def decide_order(**context):
    """Step 4 -- detect a crossing of the rolling average.

    TODO: read rolling_avg from the previous task, fetch the last 4 prices, and
    decide:
        Buy  -- the 3 older prices were below the average, the newest is above
        Sell -- the 3 older prices were above the average, the newest is below
    Return a dict with keys orderType / currentPrice / rollingAveragePrice, or
    None when there is no signal.

    Return None rather than raising when fewer than 4 prices exist -- that is
    the normal state for the first few runs, not a failure.
    """
    ti = context["ti"]
    rolling_avg = float(ti.xcom_pull(task_ids="compute_rolling_average"))

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    rows = pg.get_records("SELECT price FROM btc_prices ORDER BY ts DESC LIMIT 4")

    # Not enough history yet -- normal for the first few runs.
    if len(rows) < 4:
        print(f"Only {len(rows)} prices so far; need 4. No signal.")
        return None

    prices = [float(r[0]) for r in reversed(rows)]
    previous, current = prices[:-1], prices[-1]

    if all(p < rolling_avg for p in previous) and current > rolling_avg:
        order_type = "Buy"
    elif all(p > rolling_avg for p in previous) and current < rolling_avg:
        order_type = "Sell"
    else:
        print(f"No crossing: current={current}, rolling_avg={rolling_avg}")
        return None

    order = {
        "orderType": order_type,
        "currentPrice": current,
        "rollingAveragePrice": rolling_avg,
    }
    print(f"Signal: {order}")
    return order


def record_order(**context):
    """Step 5 -- write the order to disk and log it.

    TODO: if decide_order returned an order, write it as
    DATA_DIR/order_<timestamp>.json and insert a row into orders_log with
    status "created". Do nothing when there is no order.

    The JSON file is what the order_submitter DAG's FileSensor waits for.
    """
    ti = context["ti"]
    order = ti.xcom_pull(task_ids="decide_order")

    # decide_order returns None on most runs; nothing to record.
    if not order:
        print("No order to record.")
        return

    ts = pendulum.parse(ti.xcom_pull(task_ids="fetch_price")["ts"])
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.join(DATA_DIR, f"order_{ts.strftime('%Y%m%dT%H%M%S')}.json")
    with open(filename, "w") as f:
        json.dump(order, f)

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    pg.run(
        "INSERT INTO orders_log (payload, response, status) VALUES (%s, %s, %s)",
        parameters=(json.dumps(order), None, "created"),
    )
    print(f"Recorded order at {filename}")


with DAG(
    dag_id="price_trend_analyzer_tasks",
    description="STUB -- BTC price analyzer, one task per step",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["practice", "btc", "stub"],
) as dag:

    # TODO: replace each EmptyOperator below with a PythonOperator, e.g.
    #
    #     t_fetch = PythonOperator(
    #         task_id="fetch_price",
    #         python_callable=fetch_price,
    #         retries=3,   # only this step touches the network
    #     )
    #
    # Keep the task_ids exactly as they are -- xcom_pull refers to them by name.

    t_fetch = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_price,
        # Only this task talks to the network, so it gets the extra retries.
        retries=3,
    )

    t_store = PythonOperator(
        task_id="store_price",
        python_callable=store_price,
    )

    t_rolling = PythonOperator(
        task_id="compute_rolling_average",
        python_callable=compute_rolling_average,
    )

    t_decide = PythonOperator(
        task_id="decide_order",
        python_callable=decide_order,
    )

    t_record = PythonOperator(
        task_id="record_order",
        python_callable=record_order,
    )

    # Each step needs the previous one's result, so the graph is a chain.
    t_fetch >> t_store >> t_rolling >> t_decide >> t_record
