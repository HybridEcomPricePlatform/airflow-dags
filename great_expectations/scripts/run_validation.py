"""
Great Expectations v1.x validation for price_staging.clean_prices.
Runs after dbt — validates that transformations produced correct output.
Exit code 0 = all passed, exit code 1 = failures (Airflow marks task as failed).
"""
import os
import sys
from datetime import datetime, timezone

import great_expectations as gx

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "price-intel-prod")
DATASET    = "price_staging"
TABLE      = "clean_prices"
KEY_PATH   = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/price-intelligence/price-key.json")
)

connection_string = (
    f"bigquery://{PROJECT_ID}/{DATASET}"
    f"?credentials_path={KEY_PATH}"
)

print(f"Connecting to BigQuery: {PROJECT_ID}.{DATASET}.{TABLE}")

context = gx.get_context(mode="ephemeral")

datasource = context.data_sources.add_sql(
    name="bigquery_price",
    connection_string=connection_string,
)

asset = datasource.add_table_asset(name="clean_prices", table_name=TABLE)
batch_definition = asset.add_batch_definition_whole_table("full_table")
batch = batch_definition.get_batch()

suite = context.suites.add(
    gx.ExpectationSuite(name="clean_prices_suite")
)

# --- Volume ---
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=5_000, max_value=500_000)
)

# --- Completeness ---
for col in ["product_id", "site_name", "price", "scraped_at", "category"]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
    )

# --- Domain constraints ---
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="site_name",
        value_set=["jumia_ma", "electroplanet"],
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="currency",
        value_set=["MAD"],
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="availability",
        value_set=["in_stock", "out_of_stock", "unknown"],
    )
)

# --- Price sanity ---
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="price",
        min_value=1.0,
        max_value=200_000.0,
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnMeanToBeBetween(
        column="price",
        min_value=500.0,
        max_value=50_000.0,
    )
)

# --- Rating sanity (nullable) ---
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="rating",
        min_value=0.0,
        max_value=5.0,
        mostly=1.0,
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="review_count",
        min_value=0,
        max_value=1_000_000,
        mostly=1.0,
    )
)

# --- Primary grain uniqueness ---
suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["product_id", "scraped_at"],
    )
)

# ── Run validation ────────────────────────────────────────────────────────────
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="clean_prices_validation",
        data=batch_definition,
        suite=suite,
    )
)

result = validation_definition.run()

# ── Report ────────────────────────────────────────────────────────────────────
stats = result["statistics"]
now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

print(f"\n{'='*60}")
print(f"Great Expectations — clean_prices validation")
print(f"Run date  : {now}")
print(f"Table     : {PROJECT_ID}.{DATASET}.{TABLE}")
print(f"{'='*60}")
print(f"  Evaluated  : {stats['evaluated_expectations']}")
print(f"  Successful : {stats['successful_expectations']}")
print(f"  Failed     : {stats['unsuccessful_expectations']}")
print(f"  Success %  : {stats['success_percent']:.1f}%")
print(f"{'='*60}")

if not result["success"]:
    print("\nFailed expectations:")
    for r in result["results"]:
        if not r["success"]:
            exp_type = r["expectation_config"]["type"]
            col      = r["expectation_config"]["kwargs"].get("column", "table-level")
            res      = r["result"]
            print(f"\n  ❌ {exp_type}")
            print(f"     Column : {col}")
            print(f"     Result : {res}")
    sys.exit(1)

print("\n✅ All expectations passed.")
sys.exit(0)
