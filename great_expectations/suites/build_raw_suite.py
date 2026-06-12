# suites/build_raw_suite.py
import great_expectations as gx
import pandas as pd

SUITE_NAME = "price_events_raw_suite"

# Sites connus du projet (à adapter selon vos spiders)
KNOWN_SITES = ["jumia", "marjanemall", "electroplanet", "aswak_assalam"]

def build_raw_suite(context):
    context.add_or_update_expectation_suite(SUITE_NAME)

    # DataFrame "squelette" juste pour déclarer la suite (mêmes colonnes que la collection Mongo)
    columns = [
        "product_id", "product_name", "price", "currency", "scraped_at",
        "site_name", "schema_version", "availability", "rating",
        "review_count", "source_url"
    ]
    empty_df = pd.DataFrame(columns=columns)

    asset = context.sources.pandas_default.add_dataframe_asset(name="raw_skeleton")
    batch_request = asset.build_batch_request(dataframe=empty_df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )

    # --- Présence et types des champs obligatoires (miroir de ValidationPipeline.REQUIRED) ---
    for col in ["product_id", "price", "currency", "scraped_at", "site_name", "schema_version"]:
        validator.expect_column_values_to_not_be_null(col)

    validator.expect_column_to_exist("product_name")
    validator.expect_column_values_to_not_be_null("source_url")

    # --- Prix ---
    validator.expect_column_values_to_be_of_type("price", "float64")
    validator.expect_column_values_to_be_between(
        "price", min_value=0.01, max_value=500000, mostly=0.999
    )

    # --- Devise ---
    validator.expect_column_values_to_match_regex("currency", r"^[A-Z]{3}$")
    validator.expect_column_values_to_be_in_set(
        "currency", ["MAD", "EUR", "USD"], mostly=0.99
    )

    # --- Site source ---
    validator.expect_column_values_to_be_in_set(
        "site_name", KNOWN_SITES, mostly=0.98
    )

    # --- URL source ---
    validator.expect_column_values_to_match_regex(
        "source_url", r"^https?://"
    )

    # --- Disponibilité (valeurs produites par ValidationPipeline) ---
    validator.expect_column_values_to_be_in_set(
        "availability", ["in_stock", "out_of_stock", "unknown"]
    )

    # --- Rating (nullable, 0-5) ---
    validator.expect_column_values_to_be_between(
        "rating", min_value=0, max_value=5, mostly=0.95
    )

    # --- Review count (nullable, >= 0) ---
    validator.expect_column_values_to_be_between(
        "review_count", min_value=0, mostly=0.95
    )

    # --- Schema version ---
    validator.expect_column_values_to_be_in_set("schema_version", ["1.0"])

    # --- Fraîcheur des données (utile pour le flux NiFi temps réel) ---
    validator.expect_column_values_to_not_be_null("scraped_at")

    # --- Unicité fonctionnelle (équivalent statistique de DeduplicationPipeline) ---
    validator.expect_compound_columns_to_be_unique(
        column_list=["product_id", "site_name", "scraped_at"]
    )

    # --- Volume minimal attendu par batch (anti "pipeline silencieusement vide") ---
    validator.expect_table_row_count_to_be_between(min_value=1)

    validator.save_expectation_suite(discard_failed_expectations=False)
    return SUITE_NAME