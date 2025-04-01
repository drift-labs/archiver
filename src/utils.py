import asyncio
import csv
import datetime as dt
import os
import time

import aiohttp
from driftpy.events.parse import parse_logs
from solana.rpc.async_api import AsyncClient

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
    batch_size = len(batch)
    print(f"Sending batch request with {batch_size} transactions...")

    retry_count = 0
    while retry_count < max_retries:
        try:
            async with semaphore:
                print(f"Batch request in progress ({batch_size} txs)...")
                request_start = time.time()
                async with client.post(
                    url,
                    json=batch,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    request_time = time.time() - request_start
                    if response.status == 200:
                        print(f"Fetched batch successfully in {request_time:.2f}s.")
                        response_json = await response.json()
                        null_results = sum(
                            1 for tx in response_json if tx["result"] is None
                        )
                        if null_results > 0:
                            print(
                                f"Warning: {null_results}/{batch_size} transactions returned null results"
                            )
                        return response_json
                    else:
                        error_text = await response.text()
                        print(
                            f"Failed to fetch batch with status {response.status} in {request_time:.2f}s: {error_text}"
                        )
        except aiohttp.ClientError as e:
            print(f"Request failed due to client error: {e}")

        retry_count += 1
        print(
            f"Retrying batch ({retry_count}/{max_retries}) after {retry_delay}s delay..."
        )
        await asyncio.sleep(retry_delay)

    print(
        f"Max retries reached for batch with {batch_size} transactions. Batch will be marked as failed."
    )
    return None


async def get_logs(dc, chunks, rpc):
    if not chunks:
        return {}, []

    start_time = time.time()
    total_chunks = len(chunks)
    print(
        f"Fetching logs for {total_chunks} chunks with {sum(len(chunk) for chunk in chunks)} total signatures"
    )

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as client:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        req_chunks = [[get_tx_request(sig) for sig in chunk] for chunk in chunks]

        # Create tasks for each chunk
        tasks = []
        for i, req_chunk in enumerate(req_chunks):
            print(
                f"Creating task for chunk {i + 1}/{total_chunks} ({len(req_chunk)} transactions)"
            )
            task = fetch_txs_batch(
                client, req_chunk, semaphore, rpc, MAX_RETRIES, RETRY_DELAY
            )
            tasks.append(task)

        # Execute all tasks and wait for them to complete
        print(
            f"Executing {len(tasks)} tasks concurrently (max {CONCURRENT_REQUESTS} at a time)..."
        )
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

        logs = {}
        failed_sigs = []

        # Process results
        for i, (result, chunk) in enumerate(zip(chunk_results, chunks)):
            elapsed = time.time() - start_time

            # Check if the result is an exception
            if isinstance(result, Exception):
                print(
                    f"\n[{elapsed:.2f}s] Error in chunk {i + 1}/{total_chunks}: {str(result)}"
                )
                failed_sigs.extend(chunk)
                continue

            # Check if the result is None (failed batch)
            if result is None:
                print(
                    f"\n[{elapsed:.2f}s] Failed to fetch batch {i + 1}/{total_chunks} ({len(chunk)} signatures)"
                )
                failed_sigs.extend(chunk)
                continue

            # Process successful results
            print(
                f"[{elapsed:.2f}s] Processing results from chunk {i + 1}/{total_chunks}",
                end="\r",
            )
            for tx, sig in zip(result, chunk):
                try:
                    if tx["result"] is None or tx["result"]["meta"]["err"] is not None:
                        continue
                    events = parse_logs(dc.program, tx["result"]["meta"]["logMessages"])
                    for event in events:
                        logs.setdefault(sig, []).append(event)
                except Exception as e:
                    print(f"\nFailed to parse logs for signature {sig}: {e}")

        print("")  # Clear the carriage return line
        total_time = time.time() - start_time
        print(
            f"Log fetching complete in {total_time:.2f}s. Successful: {len(logs)} transactions, Failed: {len(failed_sigs)} signatures"
        )
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


async def fetch_sigs_for_subaccount(connection: AsyncClient, pubkey, args):
    start_time = time.time()
    print(f"Fetching signatures for subaccount: {pubkey}")
    found_start_date = False
    found_end_date = False
    before = None
    sigs_for_pubkey = []
    batch_count = 0

    while not found_end_date:
        batch_count += 1
        elapsed = time.time() - start_time
        print(
            f"[{elapsed:.2f}s] Searching for end date, batch #{batch_count}",
            end="\r",
        )

        sigs = await connection.get_signatures_for_address(pubkey, before=before)

        if len(sigs.value) == 0:
            print(
                f"\n[{elapsed:.2f}s] No signatures found for subaccount {pubkey} in first batch. Subaccount may not exist."
            )
            found_end_date = True
            found_start_date = True
            break

        for sig in sigs.value:
            if is_today(sig.block_time, args.end_date):
                before = sig.signature
                sigs_for_pubkey.append(str(sig.signature))
                found_end_date = True
                break
        if len(sigs.value) < 1_000:
            break
        before = sigs.value[-1].signature

    print("")  # Clear the last \r line

    while not found_start_date:
        batch_count += 1
        elapsed = time.time() - start_time
        print(
            f"[{elapsed:.2f}s] Fetching historical signatures, batch #{batch_count}, current count: {len(sigs_for_pubkey)}",
            end="\r",
        )

        sigs = await connection.get_signatures_for_address(pubkey, before=before)

        # Check if sigs.value is empty
        if len(sigs.value) == 0:
            print(
                f"\n[{elapsed:.2f}s] No more signatures found for subaccount {pubkey}. Stopping signature fetch."
            )
            found_start_date = True
            break

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

    total_time = time.time() - start_time
    print(
        f"Total signatures for subaccount: {pubkey}: {len(sigs_for_pubkey)} (completed in {total_time:.2f}s)"
    )
    return sigs_for_pubkey


async def fetch_and_parse_logs(pubkey, sigs, dc, rpc):
    if not sigs:
        print(f"No signatures to process for {pubkey}")
        return {}

    start_time = time.time()
    chunks = [sigs[i : i + 200] for i in range(0, len(sigs), 200)]
    total_chunks = len(chunks)

    print(f"Processing {len(sigs)} signatures for {pubkey} in {total_chunks} chunks")

    pubkey_logs, failed_sigs = await get_logs(dc, chunks, rpc)
    elapsed = time.time() - start_time

    print(f"Processed {len(pubkey_logs)} transactions in {elapsed:.2f}s")

    if len(failed_sigs) > 0:
        retry_start = time.time()
        print(f"Failed to fetch logs for {len(failed_sigs)} signatures - retrying")
        chunks = [failed_sigs[i : i + 200] for i in range(0, len(failed_sigs), 200)]
        failed_sig_logs, _ = await get_logs(dc, chunks, rpc)
        pubkey_logs.update(failed_sig_logs)
        retry_elapsed = time.time() - retry_start
        print(
            f"Retry complete. Successfully fetched logs for {len(failed_sig_logs)}/{len(failed_sigs)} failed signatures in {retry_elapsed:.2f}s"
        )

    total_time = time.time() - start_time
    print(
        f"Total processing time for {pubkey}: {total_time:.2f}s, transactions: {len(pubkey_logs)}"
    )
    return pubkey_logs
