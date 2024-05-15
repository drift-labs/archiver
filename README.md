*quick start*

1. install poetry
2. run `poetry shell`
3. run `poetry install`
4. run `python -m src.main` with your arguments

arguments guide:
1. --rpc: your solana rpc url: ex. "https://api.mainnet-beta.solana.com"
2. --public-key: the authority of the user account you want to archive. if you're trading under a delegate, this is the actual authority of the delegated account, not your signing pubkey ex: 4bZmj1Y3Dv5WrzRj4cxLUx6pfCfw1QV7y99f1aaPGU7y
3. --subaccounts: the subaccounts to archive for. ex. 0 1 2
4. --events: the events you want to archive for, out of EVENT_TYPES (OrderActionRecord will only archive FILLS.)  ex. OrderActionRecord DepositRecord LiquidationRecord
5. --start-date: the date you want to start archiving at, in YYYY-MM-DD format. MUST BE PROVIDED WITH --end-date ex. 2024-05-11
6. --end-date: the date you want to end archiving at, in YYYY-MM-DD format. MUST BE PROVIDED WITH --start-date ex. 2024-05-15


in the absence of both start date and end date, the current day will be used

full command with these example arguments: 
`python -m src.main --rpc "https://api.mainnet-beta.solana.com" --public-key 4bZmj1Y3Dv5WrzRj4cxLUx6pfCfw1QV7y99f1aaPGU7y --subaccounts 0 1 2 --events OrderActionRecord DepositRecord LiquidationRecord --start-date 2024-05-11 --end-date 2024-05-15`


```python
EVENT_TYPES = [
    "OrderActionRecord",
    "SettlePnlRecord",
    "InsuranceFundRecord",
    "InsuranceFundStakeRecord",
    "LiquidationRecord",
    "LPRecord",
    "FundingPaymentRecord",
]
```

