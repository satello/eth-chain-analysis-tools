"""
Util functions for interacting with geth and mongo.

Originally copied from https://github.com/alex-miller-0/Ethereum_Blockchain_Parser
"""
import pymongo
import os

from pymongo.errors import AutoReconnect
from collections import deque

DB_NAME = "blockchain"
COLLECTION = "blocks"

# mongodb
# -------
def initMongo(client):
    """
    Given a mongo client instance, create db/collection if either doesn't exist

    Parameters:
    -----------
    client <mongodb Client>

    Returns:
    --------
    <mongodb Client>
    """
    db = client[DB_NAME]
    try:
        db.create_collection(COLLECTION)
    except:
        pass
    try:
        # Index the block number so duplicate records cannot be made
        db['blocks'].create_index(
			[("number", pymongo.DESCENDING)],
			unique=True
		)
    except:
        pass

    return db[COLLECTION]


def insertMongo(client, d):
    """
    Insert a document into mongo client with collection selected.

    Params:
    -------
    client <mongodb Client>
    d <dict>

    Returns:
    --------
    error <None or str>
    """
    try:
        client.insert_one(d)
        return None
    except Exception as err:
        pass


def highestBlock(client):
    """
    Get the highest numbered block in the collection.

    Params:
    -------
    client <mongodb Client>

    Returns:
    --------
    <int>
    """
    n = client.find_one(sort=[("number", pymongo.DESCENDING)])
    if not n:
        # If the database is empty, the highest block # is 0
        return 0
    assert "number" in n, "Highest block is incorrectly formatted"
    return n["number"]


def getBlock(client, block_number):
    return client.find_one({"number": block_number})

def makeBlockQueue(client, start_block, end_block):
    """
    Form a queue of blocks that are recorded in mongo.

    Can handle roughly 20,000 at one time due to memory constraints

    Params:
    -------
    client <mongodb Client>

    Returns:
    --------
    <deque>
    """
    queue = deque()

    all_n = client.find({"number": {"$gte": start_block, "$lte": end_block}},
            sort=[("number", pymongo.ASCENDING)])
    for i in all_n:
        queue.append(i)

    return queue
