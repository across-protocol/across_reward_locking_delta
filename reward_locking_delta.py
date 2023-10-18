import json
import math
import os
import time

import pandas as pd
import web3


ACROSS_ACCELERATING_DISTRIBUTOR = {
    "address": "0x9040e41eF5E8b281535a96D9a48aCb8cfaBD9a48",
    "firstBlock": 15_977_129
}

with open("abi.json") as f:
    AAD_ABI = json.loads(f.read())

# Relevant addresses for what is rewarded
tokens = {
    "0xb0C8fEf534223B891D4A430e49537143829c4817": "ACX-LP",  # Yes
    "0x28F77208728B0A45cAb24c4868334581Fe86F95B": "WETH-LP",  # Yes
    "0x4FaBacAC8C41466117D6A38F46d08ddD4948A0cB": "DAI-LP",  # Yes
    "0xC9b09405959f63F72725828b5d449488b02be1cA": "USDC-LP",  # Yes
    "0x59C1427c658E97a7d568541DaC780b2E5c8affb4": "WBTC-LP",  # Yes
    "0xC2faB88f215f62244d2E32c8a65E8F58DA8415a5": "USDT-LP",  # Yes
    "0x36Be1E97eA98AB43b4dEBf92742517266F5731a3": "50wstETH-ACX-LP"  # No update
}


def parse_stakes(stake):
    args = stake["args"]
    out = {
        "transaction_hash": stake["transactionHash"].hex(),
        "block_number": stake["blockNumber"],
        "transaction_index": stake["transactionIndex"],
        "log_index": stake["logIndex"],
        "user": args["user"],
        "token": args["token"],
        "amount": args["amount"],
        "cumulative_balance": args["cumulativeBalance"],
        "cumulative_staked": args["tokenCumulativeStaked"]
    }

    return out

def findEvents(
    w3, event, start_block, last_block, epb,
    argument_filters={}, verbose=False
):
    """
    Finds all events of a particular kind

    Parameters
    ----------
    w3 : web3.contract.Contract
        A web3 object
    event : ContractEvent
        A particular event from the contract (i.e. contract.events.event)
    start_block : int
        A block to begin searching for transactions. One good candidate
        is the token's creation date...
    last_block : int
        The last block of transactions that you would like to consider
    epb : int
        The "maximum" number of times we think an event is likely to
        occur per block
    verbose : bool
        Whether to print information about which blocks have been
        processed

    Returns
    -------
    events :  list(dict)
        A list of dictionary objects that contains all of the event
        information
    """
    # Figure out which chain we are on and set max number of blocks
    # per query
    chainId = w3.eth.chain_id
    if chainId == 1:
        maxBlocks = 100_000
    elif chainId in [10, 8453, 42161]:
        maxBlocks = 250_000
    elif chainId == 137:
        maxBlocks = 3_500
    elif chainId in [288, 324]:
        maxBlocks = 5_000
    else:
        maxBlocks = 1_000

    # Infura can only handle 10_000 events at a time so we want to
    # ensure that 10_000 < events_per_block * nblocks
    nblocks = min(maxBlocks, math.floor(10_000 / epb))
    block_starts = range(start_block, last_block, nblocks)

    events = []
    for bs in block_starts:
        be = min(bs + nblocks - 1, last_block)

        if verbose:
            print(f"Beginning block {bs}")
            print(f"Ending block {be}")

        event_occurrences = []
        event_occurrences = event.get_logs(
            fromBlock=bs, toBlock=be, argument_filters=argument_filters
        )
        if verbose:
            print(f"Blocks {bs} to {be} contained {len(event_occurrences)} transactions")

        events.extend(event_occurrences)

    return events


if __name__ == "__main__":
    # Set-up various things
    w3 = web3.Web3(
        web3.Web3.HTTPProvider(
            os.environ.get("MAINNET_NODE")
        )
    )

    ad = w3.eth.contract(address=ACROSS_ACCELERATING_DISTRIBUTOR["address"], abi=AAD_ABI)
    execution_block = 18_272_082

    # Fetch all stake/unstake/ events
    stakes = findEvents(
        w3, ad.events.Stake,
        start_block=ACROSS_ACCELERATING_DISTRIBUTOR["firstBlock"], last_block=execution_block,
        epb=0.1
    )

    # Convert to DataFrames
    stakes_df = pd.DataFrame([parse_stakes(stake) for stake in stakes])

    # Group by
    gb = (
        stakes_df
        .sort_values(["block_number", "transaction_index", "log_index"])
        .groupby(["user", "token"])
    )
    groups = list(gb.groups.keys())

    # Determine how many outstanding rewards there are for each individual/token combination
    outstanding_rewards = pd.DataFrame(
        index=pd.MultiIndex.from_arrays([[], []], names=("user", "token")),
        columns=["pre_rewards", "post_rewards", "delta"]
    )

    for group in groups:
        user, token = group
        print(user)

        lookup_rewards = True

        # Data for only that group
        sub_df = gb.get_group(group)

        try:
            pre_reward = (
                ad
                .functions.getOutstandingRewards(token, user)
                .call(block_identifier=execution_block-1)
            )
            post_reward = (
                ad
                .functions.getOutstandingRewards(token, user)
                .call(block_identifier=execution_block+1)
            )
            delta = pre_reward - post_reward
        except:
            print("Trying again")
            time.sleep(1)
            pre_reward = (
                ad
                .functions.getOutstandingRewards(token, user)
                .call(block_identifier=execution_block-1)
            )
            post_reward = (
                ad
                .functions.getOutstandingRewards(token, user)
                .call(block_identifier=execution_block+1)
            )
            delta = pre_reward - post_reward

        outstanding_rewards.at[(user, token), "pre_rewards"] = str(pre_reward)
        outstanding_rewards.at[(user, token), "post_rewards"] = str(post_reward)
        outstanding_rewards.at[(user, token), "delta"] = str(delta)

    total_owed = (
        outstanding_rewards
        .reset_index()
        .groupby("user")
        .agg(lambda x: str(sum(map(int, x))))
    )
    total_owed_restricted = total_owed.loc[
        total_owed.map(lambda x: int(x) > 100_000000000000000000)
    ]
    total_owed_restricted.to_csv("reward_delta.csv")
