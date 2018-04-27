import sys
import csv
import requests
import json
import time
import argparse
import traceback
import asyncio
import functools
import concurrent.futures

# constants
BLOCK_NUMBER = 'eth_blockNumber'
GET_BLOCK = "eth_getBlockByNumber"
URL = "{}:{}".format("http://localhost", 8545)
CSV_NAME = 'all_addresses_%d.csv' % time.time()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--csv', required = False, help = 'Starting point of addresses')
    ap.add_argument('-s', '--start', required = True, help = 'CSV of twitter followers screen names')
    ap.add_argument('-e', '--end', required = False, help = 'Last block. Will be used to check balance')

    args = vars(ap.parse_args())

    if not args['csv'] and not args['start']:
        raise RuntimeError("provide a start block (-s) or a csv (-c)")

    seen_addresses = {}
    address_list = []

    if args['csv']:
        with open(args['csv']) as f:
            reader = csv.reader(f)
            for row in reader:
                address = row[0]
                seen_addresses[address] = balance
                address_list.append(address)

    start_block = int(args['start'])

    if args['end']:
        end_block = int(args['end'])
    else:
        end_block = int(rpc_request(requests.Session(), BLOCK_NUMBER, []), 16)


    block_list = []
    address_list = []

    # fetch all the blocks (I hope we don't run out of memory!)
    async def fetch_blocks(block_range):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(
                executor,
                functools.partial(
                    requests.post,
                    URL,
                    headers={"content-type": "application/json"},
                    data=json.dumps({
                        "method": GET_BLOCK,
                        "params": [hex(i), True],
                        "jsonrpc": "2.0",
                        "id": 0
                    })
                )
            ) for i in block_range]

            for response in await asyncio.gather(*futures):
                resp = response.json()
                if not resp.get('result'):
                    raise RuntimeError(resp)
                block_list.append(resp['result'])

    async def process_blocks(blocks):
        def _process_block(block):
            for tx in block['transactions']:
                sender = tx["to"]
                reciever = tx["from"]
                # TODO check if contract 'eth_getCode'
                for addr in [sender, reciever]:
                    if not addr:
                        continue
                    if not seen_addresses.get(addr, None):
                        seen_addresses[addr] = True
                        address_list.append(addr)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(
                executor,
                _process_block,
                block
            ) for block in blocks]

            await asyncio.wait(tasks)

    loop = asyncio.get_event_loop()
    start_time = time.time()
    chunk_divisor = 1500
    for i in range(int((end_block - start_block) / chunk_divisor)):
        start_place = start_block + (i * chunk_divisor)
        end_place = end_block if end_block > start_place + chunk_divisor else start_place + chunk_divisor
        loop.run_until_complete(fetch_blocks(list(range(start_place, end_place))))
    loop.run_until_complete(process_blocks(block_list))
    end_time = time.time()
    print("Took %d seconds to fetch addresses from %d blocks" % (end_time - start_time, end_block - start_block))

    address_csv = open(CSV_NAME, 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([start_block])
    for addr in address_list:
        address_writer.writerow([addr])
