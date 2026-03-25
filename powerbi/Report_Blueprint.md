# Power BI Report Blueprint

## Page 1: Business Monitoring

Top KPI strip:
- Active Customers
- Active Accounts
- Transaction Count
- Current Total Balance
- Total Amount
- Completion Rate %
- Average Transaction Amount
- Net Flow

Middle visuals:
- Line chart: Transaction Count by `PBI_DIM_DATE[DATE_DAY]`
- Clustered column chart: Total Amount by `TRANSACTION_TYPE`
- Donut chart: Transaction Count by `STATUS`
- Bar chart: Current Total Balance by `ACCOUNT_TYPE`
- Table: latest transactions

Slicers:
- `PBI_DIM_DATE[YEAR_MONTH]`
- `PBI_FACT_TRANSACTIONS'[TRANSACTION_TYPE]`
- `PBI_FACT_TRANSACTIONS'[STATUS]`
- `PBI_DIM_ACCOUNTS_CURRENT'[ACCOUNT_TYPE]`

## Page 2: CDC and SCD2 Audit

- Card: CDC Insert Events
- Card: CDC Update Events
- Card: CDC Delete Events
- Line chart: Event Count by CDC Date and Operation
- Table: Customer SCD2 history
- Table: Account SCD2 history

## Page 3: Pipeline Health

- Card: Latest CDC Event Time
- Card: Latest Transaction Time
- Table/Matrix: `PBI_PIPELINE_HEALTH`
- CDC event trend by source table

## Relationships

Use single-direction star schema style relationships:
- `PBI_DIM_DATE[DATE_DAY]` -> `PBI_FACT_TRANSACTIONS[TRANSACTION_DATE]`
- `PBI_DIM_CUSTOMERS_CURRENT[CUSTOMER_ID]` -> `PBI_FACT_TRANSACTIONS[CUSTOMER_ID]`
- `PBI_DIM_ACCOUNTS_CURRENT[ACCOUNT_ID]` -> `PBI_FACT_TRANSACTIONS[ACCOUNT_ID]`

Avoid many-to-many and avoid bidirectional filtering unless absolutely necessary.
