# datasources/bq_datasource.py
def get_bq_asset(context, table_name, asset_name):
    datasource = context.sources.add_or_update_sql(
        name="bigquery_datasource",
        connection_string="bigquery://price-intel-prod/price_analytics"
    )
    asset = datasource.add_table_asset(
        name=asset_name,
        table_name=table_name,
        schema_name="price_analytics"
    )
    return asset