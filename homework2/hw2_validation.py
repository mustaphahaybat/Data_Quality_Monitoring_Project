# homework2/hw2_validation.py

import pandas as pd
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import requests
from config import SLACK_WEBHOOK_URL


# -----------------------------
# Pydantic Row Validation Model
# -----------------------------
class OrderModel(BaseModel):
    order_id: str
    qty: int
    amount: float
    currency: str
    ship_country: str
    date: str

    @field_validator("order_id")
    def validate_order_id(cls, v):
        if not v or str(v).strip() == "":
            raise ValueError("order_id cannot be empty")
        return v

    @field_validator("qty")
    def validate_qty(cls, v):
        if v < 0:
            raise ValueError("qty must be >= 0")
        return v

    @field_validator("amount")
    def validate_amount(cls, v):
        if v < 0:
            raise ValueError("amount must be >= 0")
        return v

    @field_validator("currency")
    def validate_currency(cls, v):
        if v != "INR":
            raise ValueError("currency must be 'INR'")
        return v

    @field_validator("ship_country")
    def validate_ship_country(cls, v):
        if v != "IN":
            raise ValueError("ship_country must be 'IN'")
        return v

    @field_validator("date")
    def validate_date(cls, v):
        try:
            datetime.strptime(v, "%m-%d-%y")
        except Exception:
            raise ValueError("date must match format %m-%d-%y")
        return v


# -----------------------------
# Slack Notification
# -----------------------------
def send_slack_message(text: str):
    payload = {"text": text}
    requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


# -----------------------------
# Main Process
# -----------------------------
def main():
    df = pd.read_csv("data/Amazon_Sale_Report.csv")

    # ------- COLUMN RE-MAPPING -------
    df = df.rename(
        columns={
            "Order ID": "order_id",
            "Qty": "qty",
            "Amount": "amount",
            "currency": "currency",
            "ship-country": "ship_country",
            "Date": "date",
        }
    )

    # Yalnızca modeldeki alanları bırak
    required_cols = ["order_id", "qty", "amount", "currency", "ship_country", "date"]
    df = df[required_cols]

    valid_rows = []
    invalid_rows = []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()

        try:
            validated = OrderModel(**row_dict)
            valid_rows.append(validated.model_dump())
        except Exception as e:
            invalid_rows.append({**row_dict, "error": str(e)})

    # Save outputs
    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    valid_df.to_csv("../data/valid_rows.csv", index=False)
    invalid_df.to_csv("../data/invalid_rows.csv", index=False)

    print("Validation Completed.")
    print(f"Valid rows: {len(valid_rows)}")
    print(f"Invalid rows: {len(invalid_rows)}")

    # Slack alert
    if len(invalid_rows) > 0:
        msg = (
            "Row-Level Validation Failed\n"
            f"Invalid rows: {len(invalid_rows)}\n"
            "First 3 errors:\n"
        )
        for r in invalid_rows[:3]:
            msg += f"- {r['error']}\n"

        send_slack_message(msg)
        print("Slack alert sent.")
    else:
        send_slack_message("Row-Level Validation Passed. All rows are valid.")
        print("Slack success message sent.")


if __name__ == "__main__":
    main()
