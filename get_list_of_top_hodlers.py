import sys
import csv
import bisect
import requests
import json

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
    start_block = int(sys.argv[1])
    if not start_block:
        raise RuntimeError("provide a start block")

    seen_addresses = {}
    sorted_list = list()

    end_block = int(rpc_request(BLOCK_NUMBER, []), 16)
    try:
        while (start_block <= end_block):
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
                        balance = int(rpc_request(method=GET_BALANCE, params=[addr, 'latest']), 16)
                        seen_addresses[addr] = balance
                        # if list length is less than limit or value is higher than the lowest element
                        if balance > 0 and (
                            len(sorted_list) < NUMBER_OF_HOLDERS or balance > seen_addresses.get(sorted_list[0].balance)
                        ):
                            hodler = Hodler(addr, balance)
                            bisect.insort(sorted_list, hodler)

            start_block += 1
    except:
        pass


    # We have found all of our addresses and balances (yay!). Time to write to a csv
    address_csv = open('top_addresses.csv', 'w')
    address_writer = csv.writer(address_csv, quoting=csv.QUOTE_ALL)
    address_writer.writerow([start_block])
    for hodler in reversed(sorted_list):
        address_writer.writerow(hodler.as_list())
