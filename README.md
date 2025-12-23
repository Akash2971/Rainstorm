## RainStorm Distributed Stream Processing System

**Team**: Group 37  
**Teammates**: akashe2, nikunja2

### Overview

**RainStorm** is a distributed stream processing system built in Go on top of the HyDFS storage and membership substrate. It allows you to define multi-stage streaming jobs, where each stage runs a user-specified executable (e.g., `transform`, `filter`, `aggregate`) across a set of tasks distributed over multiple VMs.


### Starting the System

From each VM, in the project root:

```bash
cd /Users/<user>/Desktop/uiuc_mcs/CS425/rainstorm

# Example: start on vm1, vm2, vm3 ...
go run . vm1
# in another terminal / VM
go run . vm2
# etc.
```


### CLI Commands (RainStorm-related)

From any node running the server binary, you can type commands into stdin. The most relevant for RainStorm are:

- **Submit a job**:

  ```bash
  RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op2_exe> ... <opN_exe> \
           <hydfs_src_directory> <hydfs_dest_filename> \
           <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>
  ```

  - `Nstages`: number of processing stages.
  - `Ntasks_per_stage`: number of parallel tasks per stage.
  - `op*_exe`: per-stage operation spec, described below.
  - `hydfs_src_directory`: HyDFS directory containing input files.
  - `hydfs_dest_filename`: HyDFS output file produced by the final stage.
  - `exactly_once`: `true`/`false`.
  - `autoscale_enabled`: `true`/`false`.
  - `INPUT_RATE`: target input rate (tuples/sec).
  - `LW`, `HW`: lower/upper watermarks for autoscaling.

- **Inspect topology / rates**:

  ```bash
  print_topology     # show stages, tasks, and assigned VMs
  print_stage_rates  # show per-stage input throughput
  list_tasks         # list all tasks and their status
  ```

All HyDFS and membership-related commands (`create`, `get`, `append`, `multiappend`, `ls`, `liststore`, etc.) are unchanged from the original system (see `README_old.md`).

### RainStorm Examples

```bash
RainStorm 2 3 ./task_executable/filter/filter:: ./task_executable/aggregate/aggregate::7 ./hydfs/input/dataset1.csv output.txt true false 100 30 40
```

```bash
RainStorm 2 3 "./task_executable/filter/filter::JANEY DR" ./task_executable/aggregate/aggregate::3 ./hydfs/input/dataset3.csv dataset2_output.txt true false 100 40 50
```

### HyDFS and Membership CLI Commands

All underlying HyDFS and membership commands are the same as in the original system:

- **`list_self`**: print this node’s membership identifier.
- **`list_mem_ids`**: show the current membership list with hashes and status.
- **`display_protocol`**: report the active `{protocol, suspicion}` pair and drop rate.
- **`switch {gossip|ping} {suspect|nosuspect}`**: change the membership protocol and suspicion mode and broadcast to peers.
- **`display_suspects`**: list nodes currently marked as suspects.
- **`drop <percentage>`**: inject simulated message loss (e.g., `drop 0.2` for 20%).
- **`leave`**: issue a voluntary leave announcement.
- **`create <local> <HyDFS>`**: upload a local file into HyDFS.
- **`get <HyDFS> <local>`**: download a HyDFS file into the local directory.
- **`append <local> <HyDFS>`**: append data from a local file to a HyDFS file.
- **`merge <HyDFS>`**: trigger metadata/file reconciliation for the named file.
- **`multiappend <HyDFS> <vm...> <local...>`**: orchestrate parallel appends from multiple VMs.
- **`printmeta <HyDFS>`**: display stored metadata (including append order) for a file.
- **`ls <HyDFS>`**: list replicas that currently store the file.
- **`liststore`**: enumerate all files stored on this VM along with their IDs.
- **`getfromreplica <vm> <HyDFS> <local>`**: fetch a replica directly from a specific VM.
