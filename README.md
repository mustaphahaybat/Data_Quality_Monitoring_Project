# Data Quality Monitoring Project

This repository demonstrates a full **Data Quality Monitoring Pipeline** across three homework assignments, showing different aspects of data validation using Python, Pandas, and Great Expectations.

---

## Homework 1: Column-Level Validation (GE Validation)
- Used **Great Expectations** for column-level checks.
- Validates column uniqueness, nulls, value ranges, and categorical sets.
- Example: `homework1/ge_validation.py`

---

## Homework 2: Row-Level Validation (Pydantic)
- Validates each row of the dataset with a **Pydantic model**.
- Checks constraints like `qty >= 0`, `currency = INR`, `ship_country = IN`.
- Saves valid and invalid rows into separate CSVs.
- Example: `homework2/hw2_validation.py`

---

## Homework 3: Integrated Pipeline & CI/CD
- Full data quality pipeline combining Homework 1 & 2.
- **Automated GitHub Actions workflow** runs validations on every push or PR to `main`.
- Slack notifications for failed validations.
- Example: `homework3/dq_pipeline.py`
- Workflow file: `.github/workflows/dq_validation.yml`

---

## Project Structure

