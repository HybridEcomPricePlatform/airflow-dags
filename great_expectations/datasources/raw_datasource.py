# datasources/raw_datasource.py
import great_expectations as gx

def get_raw_datasource(context):
    datasource = context.sources.add_or_update_pandas(
        name="price_events_datasource"
    )
    asset = datasource.add_dataframe_asset(name="price_events_asset")
    return asset