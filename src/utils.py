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
