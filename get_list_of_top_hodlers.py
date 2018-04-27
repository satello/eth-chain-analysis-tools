import sys
import csv
import bisect
import requests
import json
import time
import argparse
import traceback
import asyncio
import functools
import concurrent.futures
from multiprocessing import Process
from queue import Queue

# constants
THREADS = 2
NUMBER_OF_HOLDERS = 1000000
BLOCK_NUMBER = 'eth_blockNumber'
GET_BALANCE = "eth_getBalance"
GET_BLOCK = "eth_getBlockByNumber"
URL = "{}:{}".format("http://localhost", 8545)
CSV_NAME = 'all_addresses_%d.csv' % time.time()
# global variables
seen_addresses = {}
sorted_list = list()
address_processing_queue = None
task_queue = None
end_block = None
start_block = 0
current_estimate_block = 0
last_reported_block = 0
running = True
error = None

class Hodler:
    def __init__(self, address, balance):
        self.address = address
        self.balance = balance

    def __lt__(self, other):
        return self.balance < other.balance

    def __gt__(self, other):
        return self.balance > other.balance

    def __eq__(self, other):
        return self.balance == other.balance

    def as_list(self):
        return [self.address, self.balance]


# Queue the deletion and insertion of addresses so we don't run into any race conditions
def process_address_tuple():
    global running
    global address_processing_queue
    global sorted_list
    try:
        while running:
            address_tuple = address_processing_queue.get()
            address = address_tuple[0]
            balance = address_tuple[1]
            if balance > 0 and (
                len(sorted_list) < NUMBER_OF_HOLDERS or balance > sorted_list[0].balance
            ):
                if len(sorted_list) >= NUMBER_OF_HOLDERS:
                    del sorted_list[0] # remove first item in list
                hodler = Hodler(address, balance) # create new hodler
                bisect.insort(sorted_list, hodler) # insert hodler
            address_processing_queue.task_done()
    except:
        traceback.print_exc()
        running = False


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--csv', required = False, help = 'Subscribers CSV to cross reference')
    ap.add_argument('-s', '--start', required = True, help = 'CSV of twitter followers screen names')
    ap.add_argument('-e', '--end', required = False, help = 'Last block. Will be used to check balance')

    args = vars(ap.parse_args())
    start_block = 0

    if not args['csv'] and not args['start']:
        raise RuntimeError("provide a start block (-s) or a csv (-c)")

    if args['csv']:
        with open(args['csv']) as f:
            reader = csv.reader(f)
            # now populate seen addresses and sorted list
            for row in reader:
                address = row[0]
                balance = int(row[1])

                seen_addresses[address] = balance
                sorted_list.insert(0, Hodler(address, balance))

    start_block = int(args['start'])

    if args['end']:
        end_block = int(args['end'])
    else:
        end_block = int(rpc_request(requests.Session(), BLOCK_NUMBER, []), 16)

    address_list = []

    # fetch all the blocks (I hope we don't run out of memory!)
    async def fetch_address():
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
            ) for i in range(start_block, end_block)]

            for response in await asyncio.gather(*futures):
                resp = response.json()['results']
                for tx in resp['transactions']:
                    sender = tx["to"]
                    reciever = tx["from"]
                    # TODO check if contract 'eth_getCode'
                    for addr in [sender, reciever]:
                        if not addr:
                            continue
                        if not seen_addresses.get(addr, None):
                            address_list.append(addr)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_address())

    address_csv = open(CSV_NAME, 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([start_block])
    for addr in address_list:
        address_writer.writerow(addr)
