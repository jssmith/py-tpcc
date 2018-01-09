import json
import re
import sys

# Table format results of paramsweep.py for viewing or further analysis

def get_res(data):
    config = data["config"]
    totals = data["results"]["TxnsTotal"]
    ct = totals["Ct"]
    duration = totals["Duration"]
    res_table = [
        config["locking_mode"],
        config["journal_mode"],
        config["database"],
        config["vfs"],
        config["cache_size"],
        config["clients"],
        config["duration"],
        ct,
        duration,
        ct / duration
        ]
    return res_table

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: formatresults result_file ...")
        sys.exit(1)

    def file_lines(filename):
        with open(filename) as f:
            for line in f.readlines():
                yield line

    res_table = list([get_res(json.loads(l)) for fn in sys.argv[1:] for l in file_lines(fn)])
    res_table.sort()
    for res in res_table:
        print(res)
