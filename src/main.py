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

from src.utils import is_today, get_logs, write

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
        "--funding-rates",
        action="store_true",
        help="Include funding rates in the archive",
    )
    parser.add_argument(
        "--events", type=valid_event, nargs="+", required=True, help="Events to archive"
    )

    args = parser.parse_args()

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

    # fetch all the signatures for today for each tracked pubkey (subaccounts + authority)
    to_remove = []
    for pubkey in user_account_pubkeys:
        sigs_for_today = []
        today_done = False
        before = None
        while not today_done:
            print(
                f"fetching signatures for today, current size: {len(sigs_for_today)}",
                end="\r",
            )
            sigs = await connection.get_signatures_for_address(
                pubkey, before=before
            )  # returns in reverse chronological order
            before = sigs.value[
                -1
            ].signature  # set the signature of the last signature in the list as the "before" for the next query

            today_size_before = len(sigs_for_today)
            new_sigs_for_today = [
                str(sig.signature) for sig in sigs.value if is_today(sig.block_time)
            ]
            sigs_for_today.extend(new_sigs_for_today)
            today_size_after = len(sigs_for_today)

            # if we didn't add all 1,000, that means that there's signatures in there that aren't from today, so we're done
            today_done = today_size_after - today_size_before < 1_000
        print(
            f"\ntotal signatures for today for subaccount: {pubkey}: {len(sigs_for_today)}"
        )
        if len(sigs_for_today) == 0:
            to_remove.append(pubkey)
            continue
        else:
            sigs_by_pubkey[pubkey] = sigs_for_today

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
                    if log.name == "OrderActionRecord" and not is_variant(
                        log.data.action, "Fill"
                    ):
                        continue
                    logs_by_sig.setdefault(sig, []).append(log)
            filtered_logs_by_pubkey.setdefault(pubkey, {}).update(logs_by_sig)
        print(
            f"Filtered logs for account: {pubkey} -> {len(filtered_logs_by_pubkey[pubkey])} transactions"
        )

    # sort the logs by type
    # outer dict key is the event type, inner dict key is the pubkey, value is the [sig, logs] pairs
    sorted_logs_by_pubkey = defaultdict(lambda: defaultdict(list))
    for pubkey, log_dict in filtered_logs_by_pubkey.items():
        for sig, logs in log_dict.items():
            for log in logs:
                sorted_logs_by_pubkey[log.name][pubkey].append((sig, log))

    # Example output check
    for event_type, pubkey_dict in sorted_logs_by_pubkey.items():
        for pubkey, sig_logs in pubkey_dict.items():
            print(f"Event Type: {event_type}, Pubkey: {pubkey}, Logs: {sig_logs}")

    write(f"logs/{today}_logs.csv", sorted_logs_by_pubkey)


if __name__ == "__main__":
    asyncio.run(main())
