import json
import datetime
import requests

BLOCK_NUMBER = 'eth_blockNumber'
GET_BLOCK = 'eth_getBlockByNumber'
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

def blocknumber_binary_search(target_date):
    # fetch top block
    #upper_bound = int(rpc_request(BLOCK_NUMBER, []), 16)
    upper_bound = 4989123
    print(upper_bound)
    # lower bound is genesis block
    lower_bound = 0

    current_block = int(upper_bound / 2) # start with block at half height

    target_timestamp = int(target_date.timestamp()) # timestamp for target date

    print(hex(current_block))
    current_block_timestamp = rpc_request(GET_BLOCK, [hex(current_block), False], 'timestamp')
    while (current_block_timestamp != target_timestamp and current_block < upper_bound and current_block > lower_bound):

        # lower bound is now current block
        if (int(current_block_timestamp, 16) < target_timestamp):
            lower_bound = current_block
        else:
            upper_bound = current_block

        current_block = upper_bound - ((upper_bound - lower_bound) // 2)
        print(current_block)
        current_block_timestamp = rpc_request(GET_BLOCK, [hex(current_block), False], 'timestamp')

    return current_block

if __name__ == "__main__":
    target = datetime.datetime.now() - datetime.timedelta(days=365)

    print(blocknumber_binary_search(target))
