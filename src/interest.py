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
from driftpy.decode.utils import decode_name

from src.main import fetch_and_parse_logs
from src.utils import fetch_all_sigs_for_subaccount

spot = 0

EVENT_TYPES = [
    "DepositRecord",
    "SwapRecord",
    "OrderActionRecord"
]

async def main():
    import pandas as pd
    df = pd.read_csv("./src/settle2.csv")
    df2 = pd.read_csv("./src/deposit.csv")
    df3 = pd.read_csv("./src/trades.csv")

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
        sigs_for_pubkey = await fetch_all_sigs_for_subaccount(connection, pubkey)
        if len(sigs_for_pubkey) == 0:
            to_remove.append(pubkey)
            continue
        else:
            sigs_by_pubkey[pubkey] = sigs_for_pubkey
            print(f"earliest signature for {pubkey}: {sigs_for_pubkey[-1]}")

    print(f"Found {len(to_remove)} subaccounts with no signatures")

    for pubkey in to_remove:
        user_account_pubkeys.remove(pubkey)

    for pubkey, sigs in sigs_by_pubkey.items():
        pubkey_logs = await fetch_and_parse_logs(pubkey, sigs, dc, rpc)
        logs_by_pubkey[pubkey] = pubkey_logs
        print(
            f"Successfully fetched logs for {len(pubkey_logs)} signatures for subaccount: {pubkey}"
        )   

    filtered_by_pubkey:  dict[str, dict[str, list]] = {}

    settled_count = 0;
    settle_sigs = []
    deposit_sigs = []
    order_action_sigs = []
    for pubkey, logs in logs_by_pubkey.items():
        for sig, log_list in logs.items():
            logs_by_sig = {}
            for log in log_list:
                if log.name in EVENT_TYPES:
                    if "settle" in log.name.lower():
                        settled_count += 1
                        settle_sigs.append(str(sig))
                    if "deposit" in log.name.lower():
                        deposit_sigs.append(str(sig))
                    if "orderaction" in log.name.lower() and "fill" in str(log.data.action_explanation).lower():
                        order_action_sigs.append(str(sig))
                    logs_by_sig.setdefault(sig, []).append(log)
            filtered_by_pubkey.setdefault(pubkey, {}).update(logs_by_sig)
        print(
            f"Filtered logs for account: {pubkey} -> {len(filtered_by_pubkey.get(pubkey, {}))} transactions"
        )


    # missing_from_sigs_count = 0
    # for sig in df["txSig"]:
    #     if str(sig) not in settle_sigs:
    #         missing_from_sigs_count += 1

    # missing_from_df = []
    # for sig in settle_sigs:
    #     if str(sig) not in df["txSig"].tolist():
    #         missing_from_df.append(str(sig))

    dmissing_from_sigs = []
    dmissing_from_df = []
    for sig in df2["txSig"]:
        if str(sig) not in deposit_sigs:
            dmissing_from_sigs.append(str(sig))
        
    for sig in deposit_sigs:
        if str(sig) not in df2["txSig"].tolist():
            dmissing_from_df.append(str(sig))

    for sig in df3["txSig"].tolist():
        if sig not in order_action_sigs:
            print(sig)

    print("\n\n\n")

    for sig in order_action_sigs:
        if sig not in df3["txSig"].tolist():
            print(sig)
    # print(len(df["txSig"].tolist()))
    # print(len(settle_sigs))
    # for sig in missing_from_df:
    #     print(sig)

    # print(f"deposits csv has {len(df2['txSig'].tolist())} sigs")
    # print(f"deposits found {len(deposit_sigs)} sigs")
    # print(len(df2["txSig"].tolist()))
    # print(len(deposit_sigs))
    # for sig in dmissing_from_df:
    #     print(sig)

    sorted_logs_by_pubkey = defaultdict(lambda: defaultdict(list))
    for pubkey, log_dict in filtered_by_pubkey.items():
        for sig, logs in log_dict.items():
            for log in logs:
                sorted_logs_by_pubkey[pubkey][log.name].append((sig, log))

                                     #sub      #idx  #amt

    net_deposits_by_subaccount_and_assets: dict[str, dict[int, int]] = {}
    usdc_net_pnl_by_subaccount: dict[str, int] = {}
    current_balances_by_subaccount_and_assets: dict[str, dict[int, int]] = {}
    trade_inflows_by_subaccount_and_assets: dict[str, dict[int, int]] = {}
    trade_outflows_by_subaccount_and_assets: dict[str, dict[int, int]] = {}

    for pubkey in user_account_pubkeys:
        net_deposits_by_subaccount_and_assets.setdefault(str(pubkey), {})
        usdc_net_pnl_by_subaccount.setdefault(str(pubkey), 0)
        current_balances_by_subaccount_and_assets.setdefault(str(pubkey), {})
        trade_inflows_by_subaccount_and_assets.setdefault(str(pubkey), {})
        trade_outflows_by_subaccount_and_assets.setdefault(str(pubkey), {})

    # interest formula = -1 * (pnl + deposits - trades out + trades in - current balance) = interest aggregated for any given asset
    for pubkey, log_dict in sorted_logs_by_pubkey.items():
        for event_type, events in log_dict.items():
            for event_tuple in events:
                event = event_tuple[1]
                match str(event_type):
                    case "DepositRecord":
                        handle_deposit_record(event, str(pubkey), net_deposits_by_subaccount_and_assets)
                    case "OrderActionRecord":
                        handle_order_action_record(event, str(pubkey), trade_inflows_by_subaccount_and_assets, trade_outflows_by_subaccount_and_assets)
                    case "SwapRecord":
                        handle_swap_record(event, str(pubkey), trade_inflows_by_subaccount_and_assets, trade_outflows_by_subaccount_and_assets)
                    case _:
                        # print("Unknown event type")
                        # print(event)
                        continue

    print("spot count: ", spot)
    for pubkey, user_account in initial_user_account_snaps.items():
        for spot_position in user_account.spot_positions:
            if spot_position.scaled_balance == 0:
                continue
            market_index = spot_position.market_index
            spot_market_account = dc.get_spot_market_account(market_index)
            token_amount = get_token_amount(
                get_signed_token_amount(spot_position.scaled_balance, spot_position.balance_type),
                spot_market_account,
                spot_position.balance_type
            )
            print(f"spot position in {market_index} has {token_amount / 10 ** spot_market_account.decimals} {decode_name(spot_market_account.name)}")
            current_balances_by_subaccount_and_assets.setdefault(pubkey, {}).setdefault(market_index, 0)
            current_balances_by_subaccount_and_assets[pubkey][market_index] = token_amount
            usdc_net_pnl_by_subaccount.setdefault(pubkey, 0)
            usdc_net_pnl_by_subaccount[pubkey] = user_account.settled_perp_pnl
            
                                             #sub      #idx  #amt
    interest_by_subaccount_assets: dict[str, dict[int, int]] = {}

    for pubkey, current_balance_by_asset in current_balances_by_subaccount_and_assets.items():
        net_deposits_by_asset = net_deposits_by_subaccount_and_assets.get(pubkey, {})
        trade_inflows_by_asset = trade_inflows_by_subaccount_and_assets.get(pubkey, {})
        trade_outflows_by_asset = trade_outflows_by_subaccount_and_assets.get(pubkey, {})
        for market_index, current_balance in current_balance_by_asset.items():
            net_deposits = net_deposits_by_asset.get(market_index, 0)
            trade_inflows = trade_inflows_by_asset.get(market_index, 0)
            trade_outflows = trade_outflows_by_asset.get(market_index, 0)
            net_pnl = 0
            if market_index == 0:
                # usdc
                print(f"usdc net pnl for {pubkey}: {usdc_net_pnl_by_subaccount.get(pubkey, 0)}")
                net_pnl = usdc_net_pnl_by_subaccount.get(pubkey, 0)
            print("\n\n\n")
            print(f"market index: {market_index}")
            print(f"current balance: {current_balance}")
            print(f"net deposits: {net_deposits}")
            print(f"net pnl: {net_pnl}")
            print(f"trade inflows: {trade_inflows}")
            print(f"trade outflows: {trade_outflows}")
            interest = -1 * (net_pnl + net_deposits - trade_outflows + trade_inflows - current_balance)
            interest_by_subaccount_assets.setdefault(pubkey, {}).setdefault(market_index, 0)
            interest_by_subaccount_assets[pubkey][market_index] = interest

    for pubkey, total_interest in interest_by_subaccount_assets.items():
        total_interest_sorted = dict(sorted(total_interest.items()))
        for market_index, interest in total_interest_sorted.items():
            spot_market = dc.get_spot_market_account(market_index)
            print(f"interest for {pubkey} in {market_index}: {interest / 10 ** spot_market.decimals} {decode_name(spot_market.name)}")
    
def handle_deposit_record(event, pubkey, net_deposits_by_subaccount_and_assets):
    # print(event.data)
    market_index = int(getattr(event.data, "market_index", None))
    if market_index is None:
        return
    amount = int(getattr(event.data, "amount", None))
    if amount is None:
        return
    direction = str(getattr(event.data, "direction", None))
    if direction is None:
        return
    deposit_amount = abs(amount)
    if "withdraw" in direction.lower():
        # flip negative
        deposit_amount *= -1
        print(f"withdraw: {deposit_amount}")
    net_deposits_by_subaccount_and_assets.setdefault(str(pubkey), {}).setdefault(market_index, 0)
    net_deposits_by_subaccount_and_assets[str(pubkey)][market_index] += deposit_amount

def handle_swap_record(event, pubkey, trade_inflows, trade_outflows):
    print("SwapRecord")
    print(event.data)
    # amount in and in market index are the asset that you are swapping from
    amount_in = int(getattr(event.data, "amount_in", None))
    in_market_index = int(getattr(event.data, "in_market_index", None))

    # amount out and out market index are the asset that you are swapping to
    amount_out = int(getattr(event.data, "amount_out", None))
    out_market_index = int(getattr(event.data, "out_market_index", None))
    
    trade_outflows[pubkey].setdefault(in_market_index, 0)
    trade_inflows[pubkey].setdefault(out_market_index, 0)

    trade_outflows[pubkey][in_market_index] += amount_in
    trade_inflows[pubkey][out_market_index] += amount_out
    # total_out_by_subaccount_and_assets.setdefault(str(pubkey), {}).setdefault(in_market_index, 0)
    # total_out_by_subaccount_and_assets[str(pubkey)][in_market_index] += amount_in

    # total_in_by_subaccount_and_assets.setdefault(str(pubkey), {}).setdefault(out_market_index, 0)
    # total_in_by_subaccount_and_assets[str(pubkey)][out_market_index] += amount_out

def handle_order_action_record(event, pubkey, trade_inflows, trade_outflows):
    order_action = str(getattr(event.data, "action", None))
    if order_action is None:
        print("no action")
    if "fill" not in order_action.lower():
        return
    market_type = str(getattr(event.data, "market_type", None))
    if market_type is None:
        print("no market type")
        return
    if "spot" not in market_type.lower():
        return
    print("SpotOrderActionRecord")
    # print(event.data)
    market_index = int(getattr(event.data, "market_index", None))
    if market_index is None:
        return
    taker = str(getattr(event.data, "taker", None))
    is_taker = taker == pubkey
    attr = "taker_order_direction" if is_taker else "maker_order_direction"
    position_direction = getattr(event.data, attr, None)
    if position_direction is None:
        return
    # this is in mint precision
    baa_filled = int(getattr(event.data, "base_asset_amount_filled", None))
    # this is in usdc precision / quote precision
    qaa_filled = int(getattr(event.data, "quote_asset_amount_filled", None))
    print(f"market index: {market_index}")
    print(f"base asset amount filled: {baa_filled}")
    print(f"quote asset amount filled: {qaa_filled}")
    if baa_filled is None:
        return
    if qaa_filled is None:
        return
    # net_deposits_by_subaccount_and_assets.setdefault(pubkey, {})
    # net_deposits_by_subaccount_and_assets[pubkey].setdefault(market_index, 0)
    # net_deposits_by_subaccount_and_assets[pubkey].setdefault(0, 0)
    trade_inflows[pubkey].setdefault(market_index, 0)
    trade_outflows[pubkey].setdefault(market_index, 0)
    trade_inflows[pubkey].setdefault(0, 0)
    trade_outflows[pubkey].setdefault(0, 0)
    global spot
    spot += 1
    match "long" in str(position_direction).lower():
        case True:
            trade_inflows[pubkey][market_index] += baa_filled
            trade_outflows[pubkey][0] += qaa_filled
            # net_deposits_by_subaccount_and_assets[pubkey][market_index] += baa_filled
            # net_deposits_by_subaccount_and_assets[pubkey][0] -= qaa_filled
        case False:
            trade_inflows[pubkey][0] += qaa_filled
            trade_outflows[pubkey][market_index] += baa_filled
            # net_deposits_by_subaccount_and_assets[pubkey][market_index] -= baa_filled
            # net_deposits_by_subaccount_and_assets[pubkey][0] += qaa_filled


if __name__ == "__main__":
    asyncio.run(main())