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
from threading import Thread
from queue import Queue

# constants
NUMBER_OF_HOLDERS = 1000000
BLOCK_NUMBER = 'eth_blockNumber'
GET_BALANCE = "eth_getBalance"
GET_BLOCK = "eth_getBlockByNumber"
URL = "{}:{}".format("http://localhost", 8545)
CSV_NAME = 'top_addresses_%d.csv' % time.time()
# global variables
seen_addresses = {}
sorted_list = list()
address_processing_queue = None
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


def rpc_request(session, method, params = [], key = None):
    """Make an RPC request to geth on port 8545."""
    payload = {
        "method": method,
        "params": params,
        "jsonrpc": "2.0",
        "id": 0
    }

    res = session.post(
          URL,
          data=json.dumps(payload),
          headers={"content-type": "application/json"}).json()

    if not res.get('result'):
        running = False
        error = res
        raise RuntimeError(res)
    return res['result'][key] if key else res['result']

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

def process_block(block_number):
    global running
    global current_estimate_block
    global seen_addresses
    global address_processing_queue

    session = requests.Session()

    try:
        start_process = time.time()
        current_estimate_block = block_number
        txs = rpc_request(session=session, method=GET_BLOCK, params=[hex(block_number), True], key='transactions')
        print("Block number %d has %d txs" % (block_number, len(txs)))

        async def fetch_address_balances(address_list):
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                loop = asyncio.get_event_loop()
                futures = list(map(lambda addr: loop.run_in_executor(
                    executor,
                    functools.partial(
                        requests.post,
                        URL,
                        headers={"content-type": "application/json"},
                        data=json.dumps({
                            "method": GET_BALANCE,
                            "params": [addr, hex(end_block)],
                            "jsonrpc": "2.0",
                            "id": 0
                        })
                    )
                ), address_list))

                for response in await asyncio.gather(*futures):
                    addr = json.loads(response.request.body)['params'][0]
                    print(addr)
                    resp = response.json()
                    balance = int(resp['result'], 16)
                    seen_addresses[addr] = balance
                    # add to queue to process list writes and deletions on a single thread
                    address_processing_queue.put((addr, balance))

        addresses_to_fetch_balance = []
        for tx in txs:
            # we consider an address active if it sent or received eth in the last year
            sender = tx["to"]
            reciever = tx["from"]
            # TODO check if contract 'eth_getCode'
            for addr in [sender, reciever]:
                if not addr:
                    continue
                if not seen_addresses.get(addr, None):
                    addresses_to_fetch_balance.append(addr)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(fetch_address_balances(addresses_to_fetch_balance))
        end_process = time.time()

        print('Processing block %d took %d seconds' % (block_number, end_process - start_process))
    except:
        traceback.print_exc()
        running = False

# thread that reports on the progress every n seconds
def report_snapshot():
    global last_reported_block
    sleep_time = 30
    while running:
        # every half hour report on progress and write results in case of program failure
        print("Current Estimated block: %d" % current_estimate_block)
        print("Number of blocks processed since last snapshot: %d" % (current_estimate_block - last_reported_block))
        print("Running rate: %d blocks per second" % (float(current_estimate_block - last_reported_block) / sleep_time))
        print("Size of address queue: %d" % address_processing_queue.qsize())
        last_reported_block = current_estimate_block
        write_results_to_csv()

        time.sleep(sleep_time)

def write_results_to_csv():
    current_address_list = sorted_list.copy()
    address_csv = open(CSV_NAME, 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([start_block])
    for hodler in reversed(current_address_list):
        address_writer.writerow(hodler.as_list())

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

    # create task queue of size of all blocks
    address_processing_queue = Queue(end_block - start_block)
    # set last_reported_block for first estimate
    last_reported_block = start_block

    # start threads
    # list maintanence thread
    list_worker = Thread(target=process_address_tuple)
    list_worker.daemon = True
    list_worker.start()
    # thread for reporting
    reporter_thread = Thread(target=report_snapshot)
    reporter_thread.daemon = True
    reporter_thread.start()

    for i in range(start_block, end_block):
        # do the work
        process_block(i)

    if error:
        print("There was an error while processing the queue")
        print(error)
        print("estimated stopping point: %d" % current_estimate_block)

    # wait for all addresses to be processed
    address_processing_queue.join()
    write_results_to_csv()
