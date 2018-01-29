import json
import re
import sys

from itertools import groupby

# Table format results of paramsweep.py for viewing or further analysis

def get_res(data, which):
    config = data["config"]
    totals = data["results"]["TxnsTotal"]
    ct = totals["Ct"]
    duration = totals["Duration"]
    dts = dict([(x["Txn"], x["Ct"]/x["Time"]) for x in data["results"]["Txns"]])
    fn = {
        "total": ct / duration,
        "status": dts["ORDER_STATUS"] if "ORDER_STATUS" in dts else None
        }
    res_table = [
        config["read_weight"],
        config["locking_mode"],
        config["journal_mode"],
        config["database"],
        config["vfs"],
        config["cache_size"],
        config["clients"],
        config["duration"],
        config["iteration"],
        ct,
        duration,
        fn[which]
        ]
    return res_table

def print_avg(res_table):
    keyfn = lambda x: x[:7]
    res_table = sorted(res_table, key=keyfn)
    for k, g in groupby(res_table, keyfn):
        values = list([x[-1] for x in g if x[-1]])
        avg_value = sum(values) / len(values)
        print(k, avg_value * 60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: formatresults result_file ...")
        sys.exit(1)

    def file_lines(filename):
        with open(filename) as f:
            for line in f.readlines():
                yield line
    def print_which(which):
        res_table = list([get_res(json.loads(l), which) for fn in sys.argv[1:] for l in file_lines(fn)])
        res_table.sort()
        for res in res_table:
            print(which, res)
        print_avg(res_table)

    print_which("total")
    print_which("status")
