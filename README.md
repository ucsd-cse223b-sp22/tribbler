# Fault-Tolerant Distributed Key-Value Storage System (Lab3)

**Note:** `ucsd-cse223b-sp22/tribbler/lab/src/lab2` and `ucsd-cse223b-sp22/tribbler/lab/proto` contains the key code files for the project. The following documentation explains the major software artifacts in the project.

## Keepers
Our keepers are arranged in such a fashion that each keeper is responsible for a subset of the backends and these subsets are disjoint. We will defer the exact specification of the keepers-backends mapping and how fault tolerance amongst keepers is achieved to later part of this documentation.  

We have created a struct to represent a Keeper (found in `keeper_impl.rs`). This struct contains:
- `live_backends_list_bc`: field required to ensure replication and fault tolerance amongst backends. This list is broadcasted to each of the live backends mapped to that keeper
- `live_backends_list_kp`: this field is sent to all other keepers as part of the heartbeat message (more about the heartbeat message later)
- `max_clock_so_far`: stores the maximum logical clock value the keeper has seen so far

## Keeper-to-Keeper Communication
Communication between keepers and backends was already established in lab 2. Here, since there is potentially more than 1 keeper and each keeper is in charge of a subset of the backends, we implemented an RPC service called **KeeperSync** (found in `keeper.proto`) with 1 RPC called `getHeartbeat`. This facilitates communication between keepers. The `getHeartbeat` rpc call returns the following:
- `maxClock`: The maximum logical clock value seen by the keeper so far in his set of backends
- `liveBackList`: The list of live backends. This is the same as `live_backends_list_kp`
- `keeperIndex`: The keeper's index
- `firstBackIndex`: The index of the first backend the keeper is in charge of
- `lastBackIndex`: The index of the last backend the keeper is in charge of

Each keeper runs a thread to periodically call `getHeartbeat` on all other keepers. Thus, every keeper gets the aforementioned data from every other keeper. This allows it to update its own state, as well as enforce [fault tolerance](#fault-tolerance-amongst-keepers)

A keeper advertises failure or rebirth of a backend only after the data migration is complete. Thus, if keeper dies in middle of data migration then the keeper who takes charge of the failed keeper will be able to detect change and trigger data migration.
## Handling Keeper Failure
The keepers use the `getHeartbeat` rpc to detect whether some other keeper has died. If the `getHeartbeat` rpc fails, then the mapping of keepers to backends changes such that a keeper takes charge of the backends initially allocated to all successive deceased keepers.

## Handling Backend Failure (Data Migration)
We define the following terms with respect to a backend:
- **Primary data**: The data of which the backend is the original owner, i.e. original data
- **Secondary data** The data which is a replica of primary data (which is stored on some other backend), i.e. the copy

The keeper performs a clock synchronization amongst the backends that it is in charge of. In doing so, it can keep track of the backends that are alive/dead.  
When it detects that the backend at index `i` has died, it performs the following:
- Gathers the backends at indices `i-1`, `i+1`, and `i+2`
- Copies the secondary data of backend `i+1` to the secondary data of backend `i+2`
- Moves the secondary data of backend `i+1` to primary data of backend `i+1`
- Copies the primary data of backend `i-1` to secondary data of backend `i+1`

When it detects that the backend at index `i` has respawned, it performs the following:
- Gathers the backends at indices `i`, `i+1`, and `i+2`
- It moves backend `i+1`'s secondary data to backend `i`'s secondary data.
- It moves the subset of backend `i+1`'s primary data that belonged to backend `i` to `i+1`'s secondary data, and copies that over to `i`'s primary data, and finally removes that data from `i+2`'s secondary data.


## Lab3 BinStoreClient (wrapper over Lab2 BinStoreClient)
- Filename: lab3_bin_store_client.rs
- This is a wrapper over the bin_store_client implemented in lab2 (file: bin_store_client.rs).
- This reimplements the functionality of all the `KeyValue` and `KeyList` traits of `storage`.
- The main design decision while implementing this is to store all the update operations (`set`, `list-append`, `list-remove`) in a log in user specific bins. Thus, each user specific bin has a list named "update_log" which stores all the update operations on all the data in that bin.
- The entrypoint of this structure is a call to `new_bin_client()` similar to lab 2. This returns an object of the type `BinStore` which implements the `BinStorage` trait
- In the `BinStore` object, it computes the hash to find the backend on which the user maps. Here we have taken a Chord like approach wherein if a key maps to a particular point on the Chord circle, and if the corresponding backend is not alive, then the data of that user will be found in a successor backend which is alive.
- The replication factor in the system is `2`. Meaning for every data point of every user, there will be `2` copies in the system. For convenience, they are referred as primary and secondary copies.
- For the update operations (`set`, `list-append` and `list-remove`), the data is written in log on user specific bin in both primary and secondary copy (see section [Data Migration](#data-migration)) 
- For the read operations, an attempt is made to read data from both copies. Such combined data from both locations is then sorted and cleaned by removing duplicates before returning to the client.
- In both read and update operations, there are various cases wherein a backend might crash in between. In such scenarios, based on the position at which it fails, either the operation is written on another primary or secondary backend to ensure replication factor of `2`. 
