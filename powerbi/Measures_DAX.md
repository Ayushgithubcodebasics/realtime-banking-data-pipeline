# Recommended DAX Measures

Create a dedicated `MEASURES` table in Power BI and add these measures.

```DAX
Total Amount = SUM('PBI_FACT_TRANSACTIONS'[AMOUNT])

Transaction Count = DISTINCTCOUNT('PBI_FACT_TRANSACTIONS'[TRANSACTION_ID])

Active Customers = DISTINCTCOUNT('PBI_DIM_CUSTOMERS_CURRENT'[CUSTOMER_ID])

Active Accounts = DISTINCTCOUNT('PBI_DIM_ACCOUNTS_CURRENT'[ACCOUNT_ID])

Current Total Balance = SUM('PBI_DIM_ACCOUNTS_CURRENT'[BALANCE])

Deposit Amount =
CALCULATE(
    [Total Amount],
    'PBI_FACT_TRANSACTIONS'[TRANSACTION_TYPE] = "DEPOSIT"
)

Withdrawal Amount =
CALCULATE(
    [Total Amount],
    'PBI_FACT_TRANSACTIONS'[TRANSACTION_TYPE] = "WITHDRAWAL"
)

Transfer Amount =
CALCULATE(
    [Total Amount],
    'PBI_FACT_TRANSACTIONS'[TRANSACTION_TYPE] = "TRANSFER"
)

Completed Transactions =
CALCULATE(
    [Transaction Count],
    'PBI_FACT_TRANSACTIONS'[STATUS] = "COMPLETED"
)

Pending Transactions =
CALCULATE(
    [Transaction Count],
    'PBI_FACT_TRANSACTIONS'[STATUS] = "PENDING"
)

Failed Transactions =
CALCULATE(
    [Transaction Count],
    'PBI_FACT_TRANSACTIONS'[STATUS] = "FAILED"
)

Completion Rate % =
DIVIDE([Completed Transactions], [Transaction Count], 0)

Average Transaction Amount =
DIVIDE([Total Amount], [Transaction Count], 0)

Net Flow = [Deposit Amount] - [Withdrawal Amount]

Latest Transaction Time = MAX('PBI_FACT_TRANSACTIONS'[TRANSACTION_TIME])

Latest CDC Event Time = MAX('PBI_CDC_AUDIT'[LATEST_EVENT_TIME])

CDC Insert Events =
CALCULATE(
    SUM('PBI_CDC_AUDIT'[EVENT_COUNT]),
    'PBI_CDC_AUDIT'[CDC_OPERATION] = "c"
)

CDC Update Events =
CALCULATE(
    SUM('PBI_CDC_AUDIT'[EVENT_COUNT]),
    'PBI_CDC_AUDIT'[CDC_OPERATION] = "u"
)

CDC Delete Events =
CALCULATE(
    SUM('PBI_CDC_AUDIT'[EVENT_COUNT]),
    'PBI_CDC_AUDIT'[CDC_OPERATION] = "d"
)
```
