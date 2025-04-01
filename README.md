# quick start

1. install poetry
2. run `poetry shell`
3. run `poetry install`
4. run `python -m src.main` with your arguments

arguments guide:

1. --rpc: your solana rpc url: ex. `"https://api.mainnet-beta.solana.com"`
2. --public-key: the authority of the user account you want to archive. if you're trading under a delegate, this is the actual authority of the delegated account, not your signing pubkey ex: `FetTyW8xAYfd33x4GMHoE7hTuEdWLj1fNnhJuyVMUGGa`
3. --subaccounts: the subaccounts to archive for. ex. 0 1 2
4. --events: the events you want to archive for, out of EVENT_TYPES (OrderActionRecord will only archive FILLS.)  ex. OrderActionRecord DepositRecord LiquidationRecord
5. --start-date: the date you want to start archiving at, in YYYY-MM-DD format. MUST BE PROVIDED WITH --end-date ex. 2024-05-11
6. --end-date: the date you want to end archiving at, in YYYY-MM-DD format. MUST BE PROVIDED WITH --start-date ex. 2024-05-15

in the absence of both start date and end date, the current day will be used

full command with these example arguments:
`python -m src.main --rpc "https://drift-cranking.rpcpool.com/f1ead98714b94a67f82203cce918" --public-key FetTyW8xAYfd33x4GMHoE7hTuEdWLj1fNnhJuyVMUGGa --subaccounts 0 1 2 --events OrderActionRecord DepositRecord LiquidationRecord --start-date 2025-03-11 --end-date 2025-03-15`

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

Spot interest calculations

to calculate interest received on all spot assets run `python -m src.interest` with the same arguments as the archiver command, but do not include `--events`.
