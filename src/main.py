import asyncio
import argparse
import datetime as dt

from collections import defaultdict

from solders.pubkey import Pubkey  # type: ignore
from solders.keypair import Keypair  # type: ignore

from solana.rpc.async_api import AsyncClient

from driftpy.addresses import get_user_account_public_key
from driftpy.constants.config import DRIFT_PROGRAM_ID
from driftpy.drift_client import DriftClient
from driftpy.types import is_variant
from driftpy.account_subscription_config import AccountSubscriptionConfig

from src.utils import is_today, get_logs, write, is_between

EVENT_TYPES = [
    "OrderActionRecord",
    "SettlePnlRecord",
    "DepositRecord",
    "InsuranceFundRecord",
    "InsuranceFundStakeRecord",
    "LiquidationRecord",
    "LPRecord",
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


async def main():
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

    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise argparse.ArgumentTypeError("Start date must be before end date.")

    if args.start_date is None and args.end_date is None:
        args.start_date = dt.datetime.now(dt.timezone.utc)
        args.end_date = dt.datetime.now(dt.timezone.utc)

    if (args.start_date is None and args.end_date is not None) or (
        args.start_date is not None and args.end_date is None
    ):
        raise argparse.ArgumentTypeError("Both start and end date must be provided.")

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

    archived_events = args.events

    today = dt.datetime.now(dt.timezone.utc).date()

    # these are assumed to be in subaccount order ascending
    user_account_pubkeys = [
        get_user_account_public_key(DRIFT_PROGRAM_ID, authority, subaccount)
        for subaccount in subaccounts
    ]
    user_account_pubkeys.append(authority)

    logs_by_pubkey = {}
    sigs_by_pubkey = {}
    to_remove = []

    for pubkey in user_account_pubkeys:
        print(f"Fetching signatures for subaccount: {pubkey}")
        found_start_date = False
        found_end_date = False
        before = None
        sigs_for_pubkey = []
        while not found_end_date:
            sigs = await connection.get_signatures_for_address(pubkey, before=before)
            for sig in sigs.value:
                if is_today(sig.block_time, args.end_date):
                    before = sig.signature
                    sigs_for_pubkey.append(str(sig.signature))
                    found_end_date = True
                    break
            if len(sigs.value) < 1_000:
                break
            before = sigs.value[-1].signature

        while not found_start_date:
            print(
                f"fetching signatures, current size: {len(sigs_for_pubkey)}",
                end="\r",
            )
            sigs = await connection.get_signatures_for_address(pubkey, before=before)
            before = sigs.value[-1].signature

            size_before = len(sigs_for_pubkey)
            new_sigs = [
                str(sig.signature)
                for sig in sigs.value
                if is_between(sig.block_time, args.start_date, args.end_date)
            ]
            sigs_for_pubkey.extend(new_sigs)
            size_after = len(sigs_for_pubkey)

            found_start_date = size_after - size_before < 1_000
        print(f"Total signatures for subaccount: {pubkey}: {len(sigs_for_pubkey)}")
        if len(sigs_for_pubkey) == 0:
            to_remove.append(pubkey)
            continue
        else:
            sigs_by_pubkey[pubkey] = sigs_for_pubkey

    for pubkey in to_remove:
        user_account_pubkeys.remove(pubkey)

    # fetch & parse all the logs for all the signatures for each pubkey for today
    for pubkey, sigs in sigs_by_pubkey.items():
        chunks = [sigs[i : i + 200] for i in range(0, len(sigs), 200)]
        pubkey_logs, failed_sigs = await get_logs(dc, chunks, rpc)
        if len(failed_sigs) > 0:
            print(
                f"Failed to fetch logs for {len(failed_sigs)} signatures: {failed_sigs}, retrying"
            )
            chunks = [failed_sigs[i : i + 200] for i in range(0, len(failed_sigs), 200)]
            failed_sig_logs, _ = await get_logs(dc, chunks, rpc)
            pubkey_logs.update(failed_sig_logs)
            print(
                f"Successfully fetched logs for {len(failed_sig_logs)}/{len(failed_sigs)} failed signatures"
            )
        print(pubkey)
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
