# Keepers
Our keepers are arranged in such a fashion that each keeper is responsible for a subset of the backends and these subsets are disjoint. We will defer the exact specification of the keepers-backends mapping and how fault tolerance amongst keepers is achieved to later in this section  

We have created a struct to represent a Keeper (found in `keeper_impl.rs`). This struct contains:
- `live_backends_list_bc`: field required to ensure replication and fault tolerance amongst backends. This list is broadcasted to each of the live backends mapped to that keeper
- `live_backends_list_kp`: this field is sent to all other keepers as part of the heartbeat message (more about the heartbeat message later)
- `max_clock_so_far`: stores the maximum logical clock value the keeper has seen so far

## RPC and heartbeat
Communication between keepers and backends was already established in lab 2. Here, since there is potentially more than 1 keeper and each keeper is in charge of a subset of the backends, we implemented an RPC service called **KeeperSync** (found in `keeper.proto`) with 1 RPC called `getHeartbeat`. This facilitates communication between keepers. The `getHeartbeat` rpc call returns the following:
- `maxClock`: The maximum logical clock value seen by the keeper so far
- `liveBackList`: The list of live backends. This is the same as `live_backends_list_kp`
- `keeperIndex`: The keeper's index
- `firstBackIndex`: The index of the first backend the keeper is in charge of
- - `lastBackIndex`: The index of the last backend the keeper is in charge of

Each keeper runs a thread to periodically call `getHeartbeat` on all other keepers. Thus, every keeper gets the aforementioned data from every other keeper. This allows it to update its own state, as well as enforce [fault tolerance](#fault-tolerance-amongst-keepers)

## Fault tolerance amongst keepers
The keepers use the `getHeartbeat` rpc to detect whether some other keeper has died. If the `getHeartbeat` rpc fails, then the mapping of keepers to backends changes such that a keeper takes charge of the backends initially allocated to all successive deceased keepers.

## Data migration
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