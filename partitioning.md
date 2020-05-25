# Notes for partitioning the TPC-C workload.

1. Start out by creating the tpcc-initial database.
2. Next use SQLite's dump command to dump each table to a separate file.
3. Use the [tpccsplit](https://github.com/jssmith/cffseval/tree/gen_scripts/gen/tpccsplit) program in cffseval to split the dump files into per-database files.
4. Import the dumps into new SQLite databases using the script below.


Import script

```
#!/bin/bash

for p in $(seq 1 32); do
	for t in $(cat tables.txt); do
		echo "import $t to partition $p"
		cat $t-$p.dump | sqlite3 db-$p.dat
	done
done
```

Here is sample SQLite syntax for attaching to a database partition.

Just run `sqlite3` then type the following.

```
attach database /tpcc-split/dbp/db-10.dat as p10;
SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM p10.WAREHOUSE WHERE W_ID = 10;
```

This Docker command is useful for working with TPC-C splits
```
docker run -it --rm \
    -v $WORKDIR/tpcc-split:/tpcc-split \
    -v $WORKDIR/cffseval/task/tpcc/py-tpcc:/py-tpcc \
    lambci/lambda:build-python3.7 /bin/bash
```