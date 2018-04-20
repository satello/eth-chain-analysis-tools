import sys
import csv
import bisect
import requests
import json
import time
import argparse

from pymongo import MongoClient
from tools.mongo import initMongo, insertMongo, highestBlock

BLOCK_NUMBER = 'eth_blockNumber'
GET_BLOCK = "eth_getBlockByNumber"
URL = "{}:{}".format("http://localhost", 8545)


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
    ap.add_argument('-s', '--start', required = False, help = 'CSV of twitter followers screen names')

    args = vars(ap.parse_args())

    if not args['start']:
        raise RuntimeError("provide a start block (-s)")

    start_block = int(args['start'])
    end_block = int(rpc_request(BLOCK_NUMBER, []), 16)

    mongo_client = initMongo(Client())
    highest_mongo = highestBlock(mongo_client)

    if highest_mongo > start_block:
        start_block = highest_mongo

    # set up basic progress bar
    sys.stdout.write("  %")
    sys.stdout.flush()

    while (start_block <= end_block):
        time.sleep(0.001)
        # write progress to bar
        sys.stdout.write("\b" * (4))
        sys.stdout.write("%d" % int((start_block / end_block) * 100))
        sys.stdout.flush()
        # fetch block
        block = rpc_request(method=GET_BLOCK, params=[hex(start_block), True])
        # add to mongo
        insertMongo(mongo_client, block)

        start_block += 1


    print("\nMongo Updated!")
