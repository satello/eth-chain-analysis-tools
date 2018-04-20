import sys
import csv
import bisect
import requests
import json
import time
import argparse

NUMBER_OF_HOLDERS = 1000000
BLOCK_NUMBER = 'eth_blockNumber'
GET_BALANCE = "eth_getBalance"
GET_BLOCK = "eth_getBlockByNumber"
URL = "{}:{}".format("http://localhost", 8545)

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


def rpc_request(method, params = [], key = None):
    """Make an RPC request to geth on port 8545."""
    payload = {
        "method": method,
        "params": params,
        "jsonrpc": "2.0",
        "id": 0
    }

    res = requests.post(
          URL,
          data=json.dumps(payload),
          headers={"content-type": "application/json"}).json()

    return res['result'][key] if key else res['result']

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--csv', required = False, help = 'Subscribers CSV to cross reference')
    ap.add_argument('-s', '--start-block', required = False, help = 'CSV of twitter followers screen names')

    args = vars(ap.parse_args())
    start_block = 0
    seen_addresses = {}
    sorted_list = list()

    if not args['csv'] and not args['start-block']:
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

    end_block = int(rpc_request(BLOCK_NUMBER, []), 16)

    # set up basic progress bar
    sys.stdout.write("  %")
    sys.stdout.flush()

    try:
        while (start_block <= end_block):
            time.sleep(0.001)
            # write progress to bar
            sys.stdout.write("\b" * (4))
            sys.stdout.write("%d" % int((start_block / end_block) * 100))
            sys.stdout.flush()
            # do the work
            txs = rpc_request(method=GET_BLOCK, params=[hex(start_block), True], key='transactions')
            for tx in txs:
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
                            del sorted_list[0] # remove first item in list
                            hodler = Hodler(addr, balance) # create new hodler
                            bisect.insort(sorted_list, hodler) # insert hodler

            start_block += 1
    except:
        print(sys.exc_info()[0])
        pass


    # We have found all of our addresses and balances (yay!). Time to write to a csv
    address_csv = open('top_addresses.csv', 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([start_block])
    for hodler in reversed(sorted_list):
        address_writer.writerow(hodler.as_list())
