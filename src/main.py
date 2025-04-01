import argparse
import asyncio
import datetime as dt
from collections import defaultdict

from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.addresses import get_user_account_public_key
from driftpy.constants.config import DRIFT_PROGRAM_ID
from driftpy.drift_client import DriftClient
from driftpy.types import is_variant
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair  # type: ignore
from solders.pubkey import Pubkey  # type: ignore

from src.utils import (
    fetch_and_parse_logs,
    fetch_sigs_for_subaccount,
    write,
)

EVENT_TYPES = [
    "OrderActionRecord",
    "SettlePnlRecord",
    "DepositRecord",
    "InsuranceFundRecord",
    "InsuranceFundStakeRecord",
    "LiquidationRecord",
    "LPRecord",
    "WithdrawRecord",
    "FundingPaymentRecord",
]


def valid_event(event):
    if event not in EVENT_TYPES:
        raise argparse.ArgumentTypeError(f"{event} is not a valid event type.")
    return event


def parse_date(date_string) -> dt.datetime:
    try:
        date_format = "%Y-%m-%d"
        parsed_date = dt.datetime.strptime(date_string, date_format)
        return parsed_date
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Not a valid date: '{date_string}'. Expected format: MM-DD-YYYY"
        )


def validate_args(args):
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise argparse.ArgumentTypeError("Start date must be before end date.")

    if args.start_date is None and args.end_date is None:
        args.start_date = dt.datetime.now(dt.timezone.utc)
        args.end_date = dt.datetime.now(dt.timezone.utc)

    if (args.start_date is None and args.end_date is not None) or (
        args.start_date is not None and args.end_date is None
    ):
        raise argparse.ArgumentTypeError("Both start and end date must be provided.")


async def main():
    print(f"Archiver starting at {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    parser = argparse.ArgumentParser(description="Parameters for archival.")
    parser.add_argument("--rpc", type=str, required=True, help="Solana RPC url")
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
        "--events", type=valid_event, nargs="+", required=True, help="Events to archive"
    )
    parser.add_argument(
        "--start-date", type=parse_date, help="Start date for the archive"
    )
    parser.add_argument("--end-date", type=parse_date, help="End date for the archive")

    args = parser.parse_args()
    print("Arguments parsed successfully")

    validate_args(args)

    print(f"Archiving for authority: {args.public_key}")
    print(f"Subaccounts to process: {args.subaccounts}")
    print(f"Event types to archive: {args.events}")
    print(
        f"Date range: {args.start_date.strftime('%Y-%m-%d')} to {args.end_date.strftime('%Y-%m-%d')}"
    )

    kp = Keypair()  # throwaway, doesn't matter

    rpc = args.rpc
    connection = AsyncClient(rpc)
    dc = DriftClient(
        connection, kp, account_subscription=AccountSubscriptionConfig("cached")
    )
    print("Subscribing to drift client...")
    await dc.subscribe()
    print("Done.\n")

    authority = Pubkey.from_string(args.public_key)
    subaccounts = args.subaccounts
    subaccounts.sort()

    archived_events = args.events

    today = dt.datetime.now(dt.timezone.utc).date()

    # these are assumed to be in subaccount order ascending
    print(f"User account addresses for authority {authority}:")
    user_account_pubkeys = [
        get_user_account_public_key(DRIFT_PROGRAM_ID, authority, subaccount)
        for subaccount in subaccounts
    ]
    user_account_pubkeys.append(authority)

    for i, subaccount_index in enumerate(subaccounts):
        print(f"- Subaccount {subaccount_index}: {user_account_pubkeys[i]}")
    print(f"- Authority: {authority}\n")

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

    # fetch & parse all the logs for all the signatures for each pubkey for today
    for pubkey, sigs in sigs_by_pubkey.items():
        pubkey_logs = await fetch_and_parse_logs(pubkey, sigs, dc, rpc)
        logs_by_pubkey[pubkey] = pubkey_logs
        print(
            f"Successfully fetched logs for {len(pubkey_logs)} signatures for subaccount: {pubkey}"
        )

    # filter out the logs that aren't in the list of events to archive
    # outer dict key is the pubkey, inner dict key is the signature, value is the list of logs
    filtered_logs_by_pubkey: dict[str, dict[str, list]] = {}
    for pubkey, logs in logs_by_pubkey.items():
        for sig, log_list in logs.items():
            logs_by_sig = {}
            for log in log_list:
                if log.name in archived_events:
                    if log.name == "OrderActionRecord" and (
                        not is_variant(log.data.action, "Fill")
                        or not str(pubkey) in str(log.data)
                    ):
                        continue
                    logs_by_sig.setdefault(sig, []).append(log)
            filtered_logs_by_pubkey.setdefault(pubkey, {}).update(logs_by_sig)
        print(
            f"Filtered logs for account: {pubkey} -> {len(filtered_logs_by_pubkey.get(pubkey, {}))} transactions"
        )

    # sort the logs by type
    # outer dict key is the event type, inner dict key is the pubkey, value is the [sig, logs] pairs
    sorted_logs_by_pubkey = defaultdict(lambda: defaultdict(list))
    for pubkey, log_dict in filtered_logs_by_pubkey.items():
        for sig, logs in log_dict.items():
            for log in logs:
                sorted_logs_by_pubkey[log.name][pubkey].append((sig, log))

    # Example output check
    # for event_type, pubkey_dict in sorted_logs_by_pubkey.items():
    #     for pubkey, sig_logs in pubkey_dict.items():
    #         print(f"Event Type: {event_type}, Pubkey: {pubkey}, Logs: {sig_logs}")

    if args.start_date and args.end_date:
        today = f"{args.start_date.strftime('%Y-%m-%d')}_{args.end_date.strftime('%Y-%m-%d')}"
    write(f"logs/{today}_logs.csv", sorted_logs_by_pubkey)


if __name__ == "__main__":
    asyncio.run(main())
