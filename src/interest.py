import asyncio
import argparse

from collections import defaultdict

from solders.pubkey import Pubkey  # type: ignore
from solders.keypair import Keypair  # type: ignore

from solana.rpc.async_api import AsyncClient

from driftpy.addresses import get_user_account_public_key
from driftpy.constants.config import DRIFT_PROGRAM_ID
from driftpy.drift_client import DriftClient
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.math.spot_market import get_signed_token_amount, get_token_amount

from src.main import parse_date, validate_args, fetch_sigs_for_subaccount, fetch_and_parse_logs

EVENT_TYPES = [
    "DepositRecord",
    "SettlePnlRecord",  
    "SwapRecord",

]
async def main():
    parser = argparse.ArgumentParser(description='Parameters for interest calculations')
    parser.add_argument('--rpc', type=str, required=True, help='Solana RPC url')
    parser.add_argument(
        "--public-key",
        type=str,
        required=True,
        help="Signing keypair of the user account to archive",
    )
    parser.add_argument(
        "--subaccounts",
        type=int,
        nargs="+",
        required=True,
        help="Subaccounts to archive",
    )
    parser.add_argument(
        "--start-date", type=parse_date, help="Start date for the archive"
    )
    parser.add_argument("--end-date", type=parse_date, help="End date for the archive")

    args = parser.parse_args()

    validate_args(args)

    kp = Keypair()  # throwaway, doesn't matter

    rpc = args.rpc
    connection = AsyncClient(rpc)
    dc = DriftClient(
        connection, kp, account_subscription=AccountSubscriptionConfig("cached")
    )
    await dc.subscribe()

    authority = Pubkey.from_string(args.public_key)
    subaccounts = args.subaccounts
    subaccounts.sort()  

    user_account_pubkeys = [
        get_user_account_public_key(DRIFT_PROGRAM_ID, authority, subaccount)
        for subaccount in subaccounts
    ]

    initial_user_account_ais = {str(pubkey): (await connection.get_account_info(pubkey)).value for pubkey in user_account_pubkeys}
    initial_user_account_snaps = {pubkey: dc.program.coder.accounts.decode(account.data) for pubkey, account in initial_user_account_ais.items()}

    logs_by_pubkey = {}
    sigs_by_pubkey = {}
    to_remove = []

    for pubkey in user_account_pubkeys:
        sigs_for_pubkey = await fetch_sigs_for_subaccount(connection, pubkey, args)
        if len(sigs_for_pubkey) == 0:
            to_remove.append(pubkey)
            continue
        else:
            sigs_by_pubkey[pubkey] = sigs_for_pubkey

    for pubkey in to_remove:
        user_account_pubkeys.remove(pubkey)

    for pubkey, sigs in sigs_by_pubkey.items():
        pubkey_logs = await fetch_and_parse_logs(pubkey, sigs, dc, rpc)
        logs_by_pubkey[pubkey] = pubkey_logs
        print(
            f"Successfully fetched logs for {len(pubkey_logs)} signatures for subaccount: {pubkey}"
        )

    filtered_by_pubkey:  dict[str, dict[str, list]] = {}

    for pubkey, logs in logs_by_pubkey.items():
        for sig, log_list in logs.items():
            logs_by_sig = {}
            for log in log_list:
                if log.name in EVENT_TYPES:
                    logs_by_sig.setdefault(sig, []).append(log)
            filtered_by_pubkey.setdefault(pubkey, {}).update(logs_by_sig)
        print(
            f"Filtered logs for account: {pubkey} -> {len(filtered_by_pubkey.get(pubkey, {}))} transactions"
        )

    sorted_logs_by_pubkey = defaultdict(lambda: defaultdict(list))
    for pubkey, log_dict in filtered_by_pubkey.items():
        for sig, logs in log_dict.items():
            for log in logs:
                sorted_logs_by_pubkey[pubkey][log.name].append((sig, log))

                                     #sub      #idx  #amt
    totals_by_subaccount_assets: dict[str, dict[int, int]] = {}
    total_withdrawals_by_subaccount_assets: dict[str, dict[int, int]] = {}

    for pubkey, log_dict in sorted_logs_by_pubkey.items():
        for event_type, events in log_dict.items():
            for event_tuple in events:
                event = event_tuple[1]
                match str(event_type):
                    case "DepositRecord":
                        market_index = int(getattr(event.data, "market_index", None))
                        if market_index is None:
                            continue
                        amount = int(getattr(event.data, "amount", None))
                        if amount is None:
                            continue
                        direction = str(getattr(event.data, "direction", None))
                        if direction is None:
                            continue
                        if "withdraw" in direction.lower():
                            amount = -abs(amount)
                            total_withdrawals_by_subaccount_assets.setdefault(str(pubkey), {}).setdefault(market_index, 0)
                            total_withdrawals_by_subaccount_assets[str(pubkey)][market_index] += amount
                        elif "deposit" in direction.lower():
                            totals_by_subaccount_assets.setdefault(str(pubkey), {}).setdefault(market_index, 0)
                            totals_by_subaccount_assets[str(pubkey)][market_index] += amount
                    
                    case "SettlePnlRecord":
                        amount = int(getattr(event.data, "pnl", None))
                        if amount is None:
                            continue
                        totals_by_subaccount_assets.setdefault(str(pubkey), {}).setdefault(0, 0)
                        totals_by_subaccount_assets[str(pubkey)][0] += amount
                    case "SwapRecord":
                        amount_out = int(getattr(event.data, "amount_out", None))
                        amount_in = -abs(int(getattr(event.data, "amount_in", None)))
                        out_market_index = int(getattr(event.data, "out_market_index", None))
                        in_market_index = int(getattr(event.data, "in_market_index", None))

                        total_withdrawals_by_subaccount_assets.setdefault(str(pubkey), {}).setdefault(in_market_index, 0)
                        total_withdrawals_by_subaccount_assets[str(pubkey)][in_market_index] += amount_in
                        totals_by_subaccount_assets.setdefault(str(pubkey), {}).setdefault(out_market_index, 0)
                        totals_by_subaccount_assets[str(pubkey)][out_market_index] += amount_out
                    case _:
                        print("Unknown event type")
                        print(event)
                        continue

                                         #sub      #idx  #amt
    initial_by_subaccount_assets: dict[str, dict[int, int]] = {}

    for pubkey, user_account in initial_user_account_snaps.items():
        for spot_position in user_account.spot_positions:
            market_index = spot_position.market_index
            spot_market_account = dc.get_spot_market_account(market_index)
            token_amount = get_token_amount(
                get_signed_token_amount(spot_position.scaled_balance, spot_position.balance_type),
                spot_market_account,
                spot_position.balance_type
            )
            initial_by_subaccount_assets.setdefault(pubkey, {}).setdefault(market_index, 0)
            initial_by_subaccount_assets[pubkey][market_index] += token_amount

    for pubkey, totals in total_withdrawals_by_subaccount_assets.items():
        for market_index, amount in totals.items():
            initial_by_subaccount_assets.setdefault(pubkey, {}).setdefault(market_index, 0)
            initial_by_subaccount_assets[pubkey][market_index] += amount

                                             #sub      #idx  #amt
    interest_by_subaccount_assets: dict[str, dict[int, int]] = {}

    for pubkey, totals in initial_by_subaccount_assets.items():
        calculated_totals = totals_by_subaccount_assets.get(pubkey, {})
        if len(calculated_totals) == 0:
            print(f"no deposits / settle pnl / swaps found for pubkey {str(pubkey)}")
            print(f"{pubkey}")
            print(calculated_totals)
            print(totals)
            continue
        for market_index, amount in totals.items():
            calculated_total = calculated_totals.get(int(market_index), None)
            if calculated_total is None:
                print(f"no deposits for {str(pubkey)} in market {market_index} found")
                continue
            print("amount")
            print(amount)
            print("calculated total")
            print(calculated_total)
            # interest = everything you've deposited and settled - everything you've withdrawn / still have in account
            interest = abs(calculated_total) - abs(amount)
            print(f"{pubkey} {market_index} {interest}")
            interest_by_subaccount_assets.setdefault(pubkey, {}).setdefault(market_index, 0)
            interest_by_subaccount_assets[pubkey][market_index] += interest

    for pubkey, total_interest in interest_by_subaccount_assets.items():
        for market_index, interest in total_interest.items():
            spot_market = dc.get_spot_market_account(market_index)
            print(f"interest for {pubkey} in {market_index}: ${interest / 10 ** spot_market.decimals}")
    
if __name__ == "__main__":
    asyncio.run(main())