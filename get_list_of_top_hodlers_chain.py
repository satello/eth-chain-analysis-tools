import sys
import csv
import bisect
import requests
import json
import time
import argparse
import traceback
import time

from tools.Hodler import Hodler
from tools.rpc import rpc_request

NUMBER_OF_HOLDERS = 1000000
BLOCK_NUMBER = 'eth_blockNumber'
GET_BALANCE = "eth_getBalance"
GET_BLOCK = "eth_getBlockByNumber"

# make unique filename for this run
FILE_NAME = 'top_addresses-%d.csv' % time.time()

def save_progress(start_time, start_block, block_number, sorted_list):
    end_time = time.time()
    print('\n')
    print("Did %d blocks in %d seconds" % (block_number - start_block, end_time - start_time))
    print("Averaged %d seconds per tx" % (tx_count // (end_time - start_time)))
    print("Averaged %d seconds per block" % ((block_number - start_block) // (end_time - start_time)))
    print("Average tx's per block %d" % (tx_count // (block_number - start_block)))
    print("last block processed %d" % block_number)
    # We have found all of our addresses and balances (yay!). Time to write to a csv
    address_csv = open(FILE_NAME, 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    for hodler in reversed(sorted_list):
        address_writer.writerow(hodler.as_list())

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--csv', required = False, help = 'An existing csv of addresses and balances')
    ap.add_argument('-s', '--start', required = True, help = 'Target start block')
    ap.add_argument('-e', '--end', required = False, help = 'Target end block')

    args = vars(ap.parse_args())
    start_block = 0
    seen_addresses = {}
    sorted_list = list()

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
        end_block = int(rpc_request(BLOCK_NUMBER, []), 16)

    block_number = start_block
    tx_count = 0

    start_time = time.time()
    try:
        for i in range(start_block, end_block):
            if i % 1000 == 0:
                # track progress and save where we are
                save_progress(start_time, start_block, block_number, sorted_list)

            block_number = i
            block = rpc_request(method=GET_BLOCK, params=[hex(i), True])
            if block_number > end_block:
                break

            tx_count += len(block['transactions'])
            # do the work
            for tx in block['transactions']:
                # we consider an address active if it sent or received eth in the last year
                sender = tx["to"]
                reciever = tx["from"]
                # TODO check if contract 'eth_getCode'
                for addr in [sender, reciever]:
                    if not addr:
                        continue
                    if not seen_addresses.get(addr, None):
                        # We haven't seen this address yet, add to list
                        balance = int(rpc_request(method=GET_BALANCE, params=[addr, 'latest']), 16)
                        seen_addresses[addr] = balance
                        # if list length is less than limit or value is higher than the lowest element
                        if balance > 0 and (
                            len(sorted_list) < NUMBER_OF_HOLDERS or balance > seen_addresses.get(sorted_list[0].balance)
                        ):
                            if len(sorted_list) >= NUMBER_OF_HOLDERS:
                                del sorted_list[0] # remove first item in list

                            hodler = Hodler(addr, balance) # create new hodler
                            bisect.insort(sorted_list, hodler) # insert hodler
    except Exception:
        traceback.print_exc()
        pass

    save_progress(start_time, start_block, block_number, sorted_list)
