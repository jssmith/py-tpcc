Notes for partitioning the TPC-C workload

customer
partition by col 3 (C_W_ID)

history
partition by col 5 (H_W_ID)

new_order
partition by col 3 (NO_W_ID)

order_line
partition by col 3 (OL_W_ID)

warehouse
partition by col 1 (W_ID)

district
partition by col 2 (D_W_ID)

item
replicate to all

orders
partition by col 4 (O_W_ID)

stock
partition by col 2 (S_W_ID)


```
#!/bin/bash

for p in $(seq 1 32); do
	for t in $(cat tables.txt); do
		echo "import $t to partition $p"
		#cat $t-$p.dump | sqlite3 db-$p.dat
	done
done
```

docker run -it --rm \
    -v /Users/jssmith/d/tpcc-split:/tpcc-split \
    -v /Users/jssmith/d/cffseval/task/tpcc/py-tpcc:/py-tpcc \
    lambci/lambda:build-python3.7 /bin/bash


attach database /tpcc-split/dbp/db-10.dat as p10;
SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM p10.WAREHOUSE WHERE W_ID = 10;