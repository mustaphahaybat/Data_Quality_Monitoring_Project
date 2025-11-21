import pandas as pd
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults
)
from great_expectations.core.batch import RuntimeBatchRequest
from slack_sdk.webhook import WebhookClient

# ---------- AYARLAR ----------
CSV_PATH = "C:/Users/HP/Desktop/Data_Quality_Monitoring_Project/data/Amazon_Sale_Report.csv"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"
GE_ROOT_DIR = "C:/Users/HP/Desktop/Data_Quality_Monitoring_Project/great_expectations"

# ---------- CSV'Yİ OKU ----------
df = pd.read_csv(CSV_PATH, low_memory=False)

# ---------- GE CONTEXT OLUŞTUR ----------
store_defaults = FilesystemStoreBackendDefaults(root_directory=GE_ROOT_DIR)

data_context_config = DataContextConfig(
    datasources={
        "my_pandas_datasource": {
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                }
            },
        }
    },
    store_backend_defaults=store_defaults,
    anonymous_usage_statistics={"enabled": False},
)

context = BaseDataContext(project_config=data_context_config)

# ---------- RUNTIME BATCH REQUEST ----------
batch_request = RuntimeBatchRequest(
    datasource_name="my_pandas_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="Amazon_Sale_Report",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# ---------- EXPECTATION SUITE OLUŞTUR ----------
suite_name = "amazon_orders_suite"
try:
    suite = context.create_expectation_suite(
        expectation_suite_name=suite_name, overwrite_existing=True
    )
except Exception:
    suite = context.get_expectation_suite(suite_name)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# ---------- EXPECTATIONS ----------
validator.expect_column_values_to_not_be_null("Order ID")
validator.expect_column_values_to_be_unique("Order ID")
validator.expect_column_values_to_be_between("Qty", min_value=0)
validator.expect_column_values_to_be_between("Amount", min_value=0)
validator.expect_column_values_to_be_in_set("Status", [
    "Shipped",
    "Shipped - Delivered to Buyer",
    "Cancelled",
    "Pending",
    "Unshipped"
])

# ---------- VALIDATE ----------
results = validator.validate()

# ---------- SONUÇLARI ANALİZ ET ----------
passed = sum(1 for r in results["results"] if r.success)
failed = sum(1 for r in results["results"] if not r.success)

# Fail olan expectations
failed_expectations = [
    r.expectation_config.expectation_type
    for r in results["results"] if not r.success
]

# Unexpected value statistics
unexpected_summary = []
for r in results["results"]:
    if not r.success:
        col_name = r.expectation_config.kwargs.get("column")
        unexpected_count = r.result.get("unexpected_count")
        if unexpected_count is not None:
            unexpected_summary.append(f"{col_name}: {unexpected_count}")

# ---------- SLACK BİLDİRİMİ ----------
summary_text = f"""
Data Validation Completed.
Passed expectations: {passed}
Failed expectations: {failed}
"""

if failed_expectations:
    summary_text += "\nFailed Expectations:\n" + "\n".join(failed_expectations)

if unexpected_summary:
    summary_text += "\nUnexpected Value Counts:\n" + "\n".join(unexpected_summary)

print(summary_text)

webhook = WebhookClient(SLACK_WEBHOOK_URL)
response = webhook.send(text=summary_text)

if response.status_code == 200:
    print("Slack notification sent successfully.")
else:
    print(f"Slack notification failed: {response.status_code}, {response.body}")
