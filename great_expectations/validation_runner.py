# validation_runner.py
import os
import pandas as pd
from pymongo import MongoClient
from ge_setup import get_context

def validate_price_events(limit=10000):
    """
    À appeler depuis un PythonOperator Airflow.
    Lève une exception si la validation échoue -> Airflow marque la tâche en FAILED.
    """
    client = MongoClient(os.environ["MONGO_URI"])
    db = client[os.environ["MONGO_DATABASE"]]
    coll = db[os.environ["MONGO_COLLECTION"]]

    cursor = coll.find().sort("scraped_at", -1).limit(limit)
    df = pd.DataFrame(list(cursor))

    if df.empty:
        raise ValueError("GE validation: aucune donnée trouvée dans price_events")

    context = get_context()

    asset = context.sources.pandas_default.add_dataframe_asset(
        name="price_events_runtime"
    )
    batch_request = asset.build_batch_request(dataframe=df)

    checkpoint = context.add_or_update_checkpoint(
        name="price_events_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": "price_events_raw_suite",
        }],
    )

    result = checkpoint.run()

    if not result["success"]:
        raise ValueError(f"GE validation FAILED pour price_events_raw_suite: {result}")

    return result


def validate_agg_prices():
    """
    À appeler après le run dbt, avant exposition au dashboard.
    """
    from datasources.bq_datasource import get_bq_asset

    context = get_context()
    asset = get_bq_asset(context, table_name="agg_daily_prices", asset_name="agg_daily_prices_runtime")
    batch_request = asset.build_batch_request()

    checkpoint = context.add_or_update_checkpoint(
        name="price_agg_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": "price_agg_dbt_suite",
        }],
    )

    result = checkpoint.run()

    if not result["success"]:
        raise ValueError(f"GE validation FAILED pour price_agg_dbt_suite: {result}")

    return result