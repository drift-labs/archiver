import asyncio
import datetime as dt
import aiohttp
import csv
import os
import time

from driftpy.events.parse import parse_logs

CONCURRENT_REQUESTS = 3  # Maximum number of concurrent requests
MAX_RETRIES = 3  # Maximum number of retries for a failed request
RETRY_DELAY = 5  # Delay in seconds between retries of requests


def is_today(unix_ts: int, today: dt.datetime) -> bool:
    ts_date = dt.datetime.fromtimestamp(unix_ts, dt.timezone.utc)

    return ts_date.date() == today.date()


def is_between(unix_ts: int, start_date: dt.datetime, end_date: dt.datetime) -> bool:
    return (
        time.mktime((start_date - +dt.timedelta(days=1)).timetuple())
        <= unix_ts
        <= time.mktime((end_date + dt.timedelta(days=1)).timetuple())
    )


def get_tx_request(sig):
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}],
    }


async def fetch_txs_batch(
    client,
    batch,
    semaphore,
    url,
    max_retries=3,
    retry_delay=5,
):
    retry_count = 0
    while retry_count < max_retries:
        try:
            async with semaphore:
                async with client.post(
                    url,
                    json=batch,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status == 200:
                        print("Fetched batch successfully.")
                        response_json = await response.json()
                        for tx in response_json:
                            if tx["result"] is None or tx["result"] == "None":
                                print("None")
                        return response_json
                    else:
                        error_text = await response.text()
                        print(
                            f"Failed to fetch batch with status {response.status}: {error_text}"
                        )
        except aiohttp.ClientError as e:
            print(f"Request failed due to client error: {e}")

        retry_count += 1
        print(f"Retrying batch ({retry_count}/{max_retries})...")
        await asyncio.sleep(retry_delay)

    print("Max retries reached for a batch. Batch will be marked as failed.")
    return None


async def get_logs(dc, chunks, rpc):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as client:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        req_chunks = [[get_tx_request(sig) for sig in chunk] for chunk in chunks]

        tasks = [
            fetch_txs_batch(client, req_chunk, semaphore, rpc, MAX_RETRIES, RETRY_DELAY)
            for req_chunk in req_chunks
        ]
        chunk_res = await asyncio.gather(*tasks)

        logs = {}
        failed_sigs = []
        for responses, sigs in zip(chunk_res, chunks):
            if responses is None:
                print(f"Failed to fetch batch for signatures: {sigs}")
                failed_sigs += sigs
                continue
            for tx, sig in zip(responses, sigs):
                if str(sig) == "2UVhmrN3PZNYjxWMxwNoSrC2NuFbTa91w3kcUELcmSkMWyd59cV27zQNk5h3dNKEEL4QGoHu2MYbQ7KGq7cSBA25":
                    print(tx)
                try:
                    if tx["result"]["meta"]["err"] is not None:
                        continue
                    events = parse_logs(dc.program, tx["result"]["meta"]["logMessages"])
                    for event in events:
                        logs.setdefault(sig, []).append(event)
                except Exception as e:
                    print(f"Failed to parse logs for signature {sig}: {e}")

        return logs, failed_sigs


def write(
    filepath: str, sorted_logs_by_pubkey: dict[str, dict[str, list[tuple[str, dict]]]]
):
    try:
        os.mkdir("logs")
    except FileExistsError:
        pass
    except OSError as error:
        print(f"Error creating directory 'logs': {error}")

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Pubkey", "Signature", "Event Type", "Log Data"])

        for event_type, pubkey_dict in sorted_logs_by_pubkey.items():
            for pubkey, sig_logs in pubkey_dict.items():
                for sig, log in sig_logs:
                    log_data_str = str(log)
                    writer.writerow([pubkey, sig, event_type, log_data_str])

async def fetch_sigs_for_subaccount(connection, pubkey, args):
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
    return sigs_for_pubkey

async def fetch_and_parse_logs(pubkey, sigs, dc, rpc):
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
    return pubkey_logs

