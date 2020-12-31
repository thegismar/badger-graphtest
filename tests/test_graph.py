from brownie import *
import os
import pytest
import time
import pandas as pd
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

KEY = os.getenv("ETHERSCAN_TOKEN")
network.connect("alchemy")

blockheight = chain.height

geyser = {
    "badger": Contract.from_explorer("0xa9429271a28F8543eFFfa136994c0839E7d7bF77"),
    "renCrv": Contract.from_explorer("0x2296f174374508278DC12b806A7f27c87D53Ca15"),
    "sbtcCrv": Contract.from_explorer("0x10fC82867013fCe1bD624FafC719Bb92Df3172FC"),
    "tbtcCrv": Contract.from_explorer("0x085A9340ff7692Ab6703F17aB5FfC917B580a6FD"),
    "uniBadgerWbtc": Contract.from_explorer(
        "0xA207D69Ea6Fb967E54baA8639c408c31767Ba62D"
    ),
    "hrenCrv": Contract.from_explorer("0xeD0B7f5d9F6286d00763b0FFCbA886D8f9d56d5e"),
    'sushibtc': Contract.from_explorer('0xB5b654efBA23596Ed49FAdE44F7e67E23D6712e7'),
    'sushieth': Contract.from_explorer('0x612f681BCd12A0b284518D42D2DBcC73B146eb65')
}

birth_block = {
    "badger": 11380946,
    "renCrv": 11380947,
    "sbtcCrv": 11380949,
    "tbtcCrv": 11380950,
    "uniBadgerWbtc": 11380951,
    "hrenCrv": 11380956,
    'sushibtc': 11539649,
    'sushieth': 11539829
}


def get_chain_data(k, v, blockheight):
    """
    :param k: name of contract/geyser
    :param v: geyser contract
    :param blockheight: latest block that we will use (constant so that we won't have anything change while iterating)
    :return: Dataframe with significant event data in columns
    """

    # i actually do not know how to do this in brownie, so web3py it is.
    contract = web3.eth.contract(v.address, abi=v.abi)
    logs_array = []

    """
    birth_block is the creation block, from there on we move 10k block up
    """
    for start in range(birth_block[k], blockheight, 10000):

        """
        this way we don't lose any blocks, 
        since alchemy restricts 10k blocks per query
        """
        end = min(start + 9999, blockheight)

        log_filter_staked = contract.events.Staked.createFilter(
            fromBlock=start, toBlock=end
        )
        log_filter_unstaked = contract.events.Unstaked.createFilter(
            fromBlock=start, toBlock=end
        )

        logs_staked = log_filter_staked.get_all_entries()
        logs_unstaked = log_filter_unstaked.get_all_entries()

        """
        here we create an array that contains rows of dictionaries which later will serve
        as columns for our pandas dataframe
        """

        for ls in logs_staked:
            a = ls["args"]
            logs_array.append(
                {
                    "event": "staked",
                    "user": str(a["user"]).lower(),
                    "amount": int(a["amount"]),
                    "timestamp": int(a["timestamp"]),
                    "block": int(a["blockNumber"]),
                }
            )

        for lu in logs_unstaked:
            a = lu["args"]
            logs_array.append(
                {
                    "event": "unstaked",
                    "user": str(a["user"]).lower(),
                    "amount": -int(a["amount"]),  # unstaked can easily be added up
                    "timestamp": int(a["timestamp"]),
                    "block": int(a["blockNumber"]),
                }
            )

    df = pd.DataFrame(logs_array)
    df.sort_values(by=["timestamp", "amount"], ascending=[False, False], inplace=True)
    return df


def get_graph_data(v, blockheight):
    """
    :param blockheight: block at which we'll query
    :param v: brownie contract / geyser
    :returns df: datafram
    """

    # gql initialization stuff
    transport = AIOHTTPTransport(
        url="https://api.thegraph.com/subgraphs/name/m4azey/badger-finance"
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)

    result = []
    # this is really important, thegraph makes all accounts lowercase!!
    v = str(v).lower()

    # doesn't have to be 100 but 100 is enough, this could be refactored
    # TODO refactor for loop
    for n in range(100):
        query = gql(
            """
            query getStaked ($skip: Int!, $geyser: String!, $blockheight: Int!) {
            
              stakedEvents(first: 1000, skip: $skip, where: {geyser: $geyser},
                orderBy: timestamp, orderDirection: desc, block: {number: $blockheight}){
                
                geyser{
                    id
                }
                user
                amount
                timestamp
                blockNumber
            }

               unstakedEvents(first: 1000, skip: $skip, where: {geyser: $geyser},
                orderBy: timestamp, orderDirection: desc, block: {number: $blockheight}){
                
                geyser{
                    id
                }
                user
                amount
                timestamp
                blockNumber
                }
            }
        """
        )

        # since thegraph only returns 1000 entries, we offset each query by n* 1000
        # v - geyser
        params = {"skip": n * 1000, "geyser": v, "blockheight": blockheight}

        try:
            graphr = client.execute(query, variable_values=params)
        except Exception as e:
            print(e)
            time.sleep(5)
            continue

        # this would mean that we don't have any items more, so the loop can end
        if len(graphr["stakedEvents"]) == 0 and len(graphr["unstakedEvents"]) == 0:
            break
        # create the array with dict to later generate pandas dataframe
        for r in graphr["stakedEvents"]:
            result.append(
                {
                    "event": "staked",
                    "user": str(r["user"]).lower(),
                    "amount": int(r["amount"]),
                    "timestamp": int(r["timestamp"]),
                    "block": int(r["blockNumber"]),
                }
            )
        for r in graphr["unstakedEvents"]:
            result.append(
                {
                    "event": "unstaked",
                    "user": str(r["user"]).lower(),
                    "amount": -int(r["amount"]),
                    "timestamp": int(r["timestamp"]),
                    "block": int(r["blockNumber"]),
                }
            )

    df = pd.DataFrame(result)
    df.sort_values(by=["timestamp", "amount"], ascending=[False, False], inplace=True)
    return df


@pytest.mark.parametrize("k, v", geyser.items())
def test_sanity_graph(k, v):
    df = get_graph_data(v, blockheight)
    df.reset_index(inplace=True, drop=True)
    sum_staked = df["amount"].sum()
    value_sc = v.totalStaked(block_identifier=blockheight)
    assert sum_staked == value_sc


@pytest.mark.parametrize("k, v", geyser.items())
def test_sanity_chain(k, v):
    df = get_chain_data(k, v, blockheight)
    sum_staked = df["amount"].sum()
    value_sc = v.totalStaked(block_identifier=blockheight)
    assert sum_staked == value_sc


@pytest.mark.parametrize("k, v", geyser.items())
def test_values(k, v):
    df_graph = get_graph_data(v, blockheight).astype(str)
    df_chain = get_chain_data(k, v, blockheight).astype(str)
    df_chain_user = df_chain["user"].copy()
    df_graph_user = df_graph["user"].copy()
    df_graph_user.sort_values(inplace=True)
    df_chain_user.sort_values(inplace=True)
    df_graph_user.reset_index(drop=True, inplace=True)
    df_chain_user.reset_index(drop=True, inplace=True)
    df_chain_block = df_chain["block"].copy()
    df_graph_block = df_graph["block"].copy()
    df_graph_block.sort_values(inplace=True)
    df_chain_block.sort_values(inplace=True)
    df_graph_block.reset_index(drop=True, inplace=True)
    df_chain_block.reset_index(drop=True, inplace=True)
    df_chain_amount = df_chain["amount"].copy()
    df_graph_amount = df_graph["amount"].copy()
    df_graph_amount.sort_values(inplace=True)
    df_chain_amount.sort_values(inplace=True)
    df_graph_amount.reset_index(drop=True, inplace=True)
    df_chain_amount.reset_index(drop=True, inplace=True)

    assert df_chain_amount.equals(df_graph_amount)
    assert df_chain_block.equals(df_graph_block)
    assert df_chain_user.equals(df_graph_user)
