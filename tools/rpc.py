import requests
import json

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

    if not res.get('result'):
        raise RuntimeError(res)

    return res['result'][key] if key else res['result']
