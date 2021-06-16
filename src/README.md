# Hotnode incorporation checklist
1) Did you change the hotnode address (`node-X:50051`) in cockroachdb 
   `pkg/server/server.go`?
   
2) Does that address match the one in your `trial_<config_object_name>.py`
in `self.hot_node`?
    
3) Did you change the hotkey threshold to be what you want in CRDB
`pkg/sql/conn_executor_prepare.go` in `isHotkey(key byte[])`?
   
4) Does that threshold match the one in `trial_<config_object_name>.py`
in `self.hot_node_threshold`?

# How to implement a new server-client test script on branch async_redo

1) Copy `src/async_config_object` and name it 
   `trial_<whatever_you_want>.py`. The `.gitignore` will
   ignore it. 
   - Make sure you populate the fields under the `#default`
    comment.
   - Make sure all methods are correctly written,
    especially the part on populating server and client nodes.
     For example, are the nodes regioned? Does it matter?
   - Make sure `generate_all_config_files()` method is implemented.
   - Make sure the concurrency of the clients is always called 
     "concurrency" (`self.concurrency` in `ConfigObject`)

2) Change the fields you need to.

3) Change `config/async_lt.ini` for latency-throughput.

4) Implement to the interface of `src/async_server.py`.
    - When implementing `aggregate_raw_logs()` function,
    make sure to use the keys
      - throughput: `"ops/sec(cum)"`
      - p50: `"p50(ms)"`
      - p99: `"p99(ms)"`
    or the latency throughput graphs won't gnuplot at all
   - Make sure gnuplot is installed (`apt install gnuplot-x11`)

5) Whatever you name your implementation from the previous
step, change the line `import async_server` in `src/async_main.py`
   to `import <whatever_you_implemented> as async_server`
    
6) Configure/implement the swath of functions at the
head of `src/async_main.py` to match your needs.
   - Make sure the directory is correct. It's set to 
    `thermopylae_tests/scratch/db_{datetime}` right now.
     
7) From `~/thermopylae_tests` directory, run `python3 src/async_main.py`
   
# How to implement new `async_determine_stable_interval.py` on branch async_redo
1) Code to the interface of `src/async_server.py`
2) Replace `import async_server` with `import <whatever_you_coded> as async_server`
3) From `~/thermopylae_tests` dir, run `python3 src/async_determine_stable_interval.py
   --duration 30s --csv_location scratch/stabilizer --graph_location scratch/stabilizer`
   
# How to add your config
1) Make a copy of `src/config_object.py` and name it `trial_<whatever_you_want>.py`.
The `.gitignore` will ignore it in the directory. 
2) Change the fields that you need to. Add ones you need.
    - You may need to implement new functionality that goes along with any new
    fields.
3) Determine what latency throughput files should match it (choose the range and
step_size). See `config/lt.ini` for the default example.
4) In `src/main.py`, add your new `trial_<whatever_you_want>.py` file with the
filepath of the latency throughput file to the configuration section. Remember to
import the config object files in `src/main.py`.
5) Make sure the sqlite database directory is what you want it to be (by default, 
it is set to to `/proj/cops-PG0/workspaces/jl87`)
6) IMPORTANT: if any nodes have crashed, make sure to add their ip_enums as *args
   in `enumerate_workload_nodes(...)` and `enumerate_warm_nodes(...)`. For example:
   `enumerate_workload_nodes(driver_node_ip_enum, num_workload_nodes, 2, 5, 6)`
   for node-1, node-4, and node-5.
   Don't forget to repeat for `enumerate_warm_nodes(...)`.
7) From the git root, run: `./src/main.py`

### Need to Implement
- Automatic start-up of chosen hotshard node.
- Extracting of the cockroach commit in the copied parameter ini files instead of
just the branch name, which may or may not exist at a further point in time.

### Not Implemented
- Partition affinity
