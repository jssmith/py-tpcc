Benchmarking Serverless SQLite
==============================

Preparation
-----------

Install python3 and build SQLite. Below we assume SQLite is built in the directory `/home/ec2-user/sqlite-build/`.

Mount the EFS mount at `/efs`.

Creating an initial database
----------------------------

For faster testing run the database load once and make a copy of it for each test.

```
LD_PRELOAD=/home/ec2-user/sqlite-build/.libs/libsqlite3.so python3 \
    tpcc.py --config initialization-config --reset --no-execute sqlite
```

Running a single benchmark
--------------------------

Create a configuration file, e.g., `my-config`, possibly using `initialization-config` as a template.


### Running on `/tmp`

Here is how to set up for a test running on the local `/tmp` file system.
```
cp /tmp/tpcc-initial /tmp/tpcc
rm -f /tmp/tpcc-nfs-wal
rm -f /tmp/tpcc-nfs-journal
sync
sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
```

Example content of `my-config`:
```
[sqlite]
journal_mode = delete
locking_mode = exclusive
database = /tmp/tpcc
cache_size = 2000
vfs = unix
```

Then run:
```
LD_PRELOAD=/home/ec2-user/sqlite-build/.libs/libsqlite3.so python3 \
    tpcc.py --config my-config --no-load sqlite
```

### Running using user space NFS client

Here is how to set up for a test running on the `/efs` EFS mount.
```
sudo -u nfsnobody -g nfsnobody cp /tmp/tpcc /efs/tpcc
sudo rm -f /efs/tpcc-nfs-wal
sudo rm -f /efs/tpcc-nfs-journal
sync
sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
```

Example content of `my-config`:
```
[sqlite]
journal_mode = delete
locking_mode = exclusive
database = 192.168.1.57/tpcc
cache_size = 2000
vfs = nfs4
```

Then run:
```
LD_PRELOAD=/home/ec2-user/sqlite-build/.libs/libsqlite3.so python3 \
    tpcc.py --config my-config --no-load sqlite
```

Running a parameter sweep
-------------------------

Create a configuration file, e.g,. `my_sweep.json`, with content like the
following
```
{
    "databases": [
        { "path": "/tmp/tpcc", "vfs": "unix" },
        { "path": "192.168.1.57/tpcc", "vfs": "nfs4" }
    ],
    "locking_modes": [ "normal" ],
    "journal_modes": [ "delete" ],
    "num_clients": [1, 2],
    "durations": [10],
    "cache_sizes": [2000, 10000],
    "iterations": 1
}
```

```
python3 paramsweep.py \
    --config my_sweep.json \
    --results results.json
```

Format output of the sweep
```
python3 formatresults.py results.json
```
