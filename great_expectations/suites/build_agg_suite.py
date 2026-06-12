# suites/build_agg_suite.py
SUITE_NAME = "price_agg_dbt_suite"

def build_agg_suite(context):
    from datasources.bq_datasource import get_bq_asset

    context.add_or_update_expectation_suite(SUITE_NAME)
    asset = get_bq_asset(context, table_name="agg_daily_prices", asset_name="agg_daily_prices_asset")

    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )

    # --- Clés de l'agrégat ---
    for col in ["product_id", "date", "site_name", "avg_price"]:
        validator.expect_column_values_to_not_be_null(col)

    # --- Cohérence statistique ---
    validator.expect_column_values_to_be_between("avg_price", min_value=0.01, max_value=500000)
    validator.expect_column_values_to_be_between("min_price", min_value=0.01)
    validator.expect_column_values_to_be_between("max_price", min_value=0.01)

    # max_price doit toujours être >= avg_price >= min_price
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        column_A="max_price", column_B="min_price", or_equal=True
    )

    # --- Volatilité (price_velocity / std) bornée pour détecter les outliers d'agrégation ---
    validator.expect_column_values_to_be_between(
        "price_volatility", min_value=0, max_value=1, mostly=0.97
    )

    # --- Pas de doublons jour/produit/site ---
    validator.expect_compound_columns_to_be_unique(
        column_list=["product_id", "site_name", "date"]
    )

    # --- Fraîcheur : la table doit contenir la date du jour ---
    import datetime
    today = datetime.date.today().isoformat()
    validator.expect_column_max_to_be_between(
        column="date", min_value=today, max_value=today
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    return SUITE_NAME