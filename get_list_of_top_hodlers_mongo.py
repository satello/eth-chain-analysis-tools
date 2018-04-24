import sys
import csv
import bisect
import requests
import json
import time
import argparse
import traceback
import time

from pymongo import MongoClient

from tools.rpc import rpc_request
from tools.Hodler import Hodler
from tools.mongo import initMongo, makeBlockQueue, getBlock

NUMBER_OF_HOLDERS = 1000000
BLOCK_NUMBER = 'eth_blockNumber'
GET_BALANCE = "eth_getBalance"
GET_BLOCK = "eth_getBlockByNumber"

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--csv', required = False, help = 'Subscribers CSV to cross reference')
    ap.add_argument('-s', '--start', required = False, help = 'Target start block')
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
            start_block = int(next(reader)[0])
            # now populate seen addresses and sorted list
            for row in reader:
                address = row[0]
                balance = int(row[1])

                seen_addresses[address] = balance
                sorted_list.insert(0, Hodler(address, balance))

    if args['start']:
        start_block = int(args['start'])

    if args['end']:
        end_block = int(args['end'])
    else:
        end_block = int(rpc_request(BLOCK_NUMBER, []), 16)

    mongo_client = initMongo(MongoClient())
    block_queue = makeBlockQueue(mongo_client, start_block, end_block)
    block_number = None

    # set up basic progress bar
    sys.stdout.write("  %")
    sys.stdout.flush()

    start_time = time.time()
    try:
        for block in block_queue:
            # block = getBlock(mongo_client, i)
            block_number = block['number']
            if block_number > end_block:
                break
            # write progress to bar
            sys.stdout.write("\b" * (4))
            sys.stdout.write("%d" % int((block_number / end_block) * 100))
            sys.stdout.flush()
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
                        balance = int(rpc_request(method=GET_BALANCE, params=[addr, hex(end_block)]), 16)
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


    end_time = time.time()
    print("Did %d iterations in %d seconds" % (block_number - start_block, end_time - start_time))
    # We have found all of our addresses and balances (yay!). Time to write to a csv
    address_csv = open('top_addresses.csv', 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([block_number])
    for hodler in reversed(sorted_list):
        address_writer.writerow(hodler.as_list())
