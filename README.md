# Graph - Chain events test

Hardcoded - current geyser Contracts, their creation blocks.

Latest block that is considered for the test. Since the tests are relatively time-consuming, state-changes are possible
from beginning to end which would give false negatives.

## Getting events from chain

Using Web3py filter for Staked / Unstaked events and with a 10k block interval (alchemy restriction) we're adding the
significant elements of the event to a dictionary that is then added to an array the elements are

* event - staked/unstaked (str)
* user - account (str)
* amount - staked value (int)
* timestamp - (int)
* block - (int)

For easy manipulation the array, containing the dictionaries, is then converted into a pandas dataframe, sorted and
returned.

## Getting events from The Graph

Using gql, a client is created, we do two queries, one for staked and one for unstaked events and have 3 variables for
each one skip- thegraph only returns 1k results so we might have to offset the first 1000, geyser - lowercase contract
string, and latest block (important, accounts need to be **lowercase**).

We check if there are no more elements to query for and like above create dictionaries, that are added to an array and added
into a Dataframe.

### Dataframes are easy to manipulate, to store and to convert 

## Tests:

### Sanity tests
Just adding all amounts of stakes (+) and unstakes (-) for each geyser, and comparing with totalStaked() value at the latest
block.

### Equality test
We split the dataframe into it's columns, sort those by value and then reindex them.
This is done in the same fashion to both dataframes, for each column, so if the columns are equal to each other,
the original data also **must** have been equal. 

All tests pass is considered a positive result.