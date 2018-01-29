import argparse
import json
import subprocess
import sys
import random
import re
import os

# The paramsweep script runs the TPC-C benchmark using a variety of
# configuration parameters.
#
# Before running this script you need to create an initial database in a
# temporary location.
#
#     LD_PRELOAD=$PATH_TO_SQLITE_BUILD/.libs/libsqlite3.so \
#         python3 tpcc.py \
#         --config initializaion-config \
#         --no-execute sqlite
#
# Experiments will all begin by copying the same initial database to a test
# location.

def init_location(location, vfs, alt_path):

    def cp(location):
        res = subprocess.call(["/bin/cp", "/tmp/tpcc-initial", location])
        if res:
            print("problem in copy")
            sys.exit(1)

    def su_cp(location):
        res = subprocess.call(["/usr/bin/sudo", "-u", "nfsnobody", "-g", "nfsnobody", "/bin/cp", "/tmp/tpcc-initial", location])
        if res:
            print("problem in copy")
            sys.exit(1)

    def su_rm(location):
        res = subprocess.call(["/usr/bin/sudo", "/bin/rm", "-f", location])
        if res:
            print("problem in remove")
            sys.exit(1)

    def su_rm_all(location):
        su_rm(location)
        su_rm("%s-wal" % location)
        su_rm("%s-journal" % location)

    if vfs == "nfs4":
        if alt_path:
            mount_location = alt_path
        else:
            p = re.compile("^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/(.*)$")
            m = p.match(location)
            if not m:
                print("failure to match on location", location)
                sys.exit(1)
            mount_location = "/efs/%s" % m.group(1)
            print("translated", location, mount_location)
        su_rm_all(mount_location)
        su_cp(mount_location)
        res = subprocess.call(["/usr/bin/sudo", "/bin/chown", "nfsnobody.nfsnobody", mount_location])
        if res:
            print("problem in chown")
            sys.exit(1)
    else:
        su_rm_all(location)
        cp(location)

    if str.startswith(sys.platform, "linux"):
        res = subprocess.call(["/bin/sync"])
        if res:
            print("problem in sync")
            sys.exit(1)
        res = subprocess.call(["/usr/bin/sudo", "/bin/bash", "-c", "echo 1 > /proc/sys/vm/drop_caches"])
        if res:
            print("problem in flush caches")
            sys.exit(1)
    else:
        print("system is not Linux so skipping cache flush")

def run_test(config_file, clients, duration=None, read_weight=None, json_output=None):
    env = os.environ
    if LD_PRELOAD not in env:
        print("must define LD_PRELOAD with path to libsqlite3.so")
        sys.exit(1)
    args = ["python3", "tpcc.py",
        "--config", config_file,
        "--clients", str(clients)]
    if duration:
        args += ["--duration", str(duration)]
    if read_weight:
        args += ["--frac-read", "%.3f" % read_weight]
    if json_output:
        args += ["--json-output", json_output]
    args += ["--no-load", "sqlite" ]
    p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res = p.communicate()
    print(res)
    return res

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='Parameter sweep for TPC-C')
    aparser.add_argument('--config', type=argparse.FileType('r'), required=True,
                         help='Path to the json configuration file')
    aparser.add_argument('--results', type=argparse.FileType('a'), required=True,
                         help='Path to the results output file')
    args = vars(aparser.parse_args())
    sweep_config = json.load(args["config"])
    results_f = args["results"]

    results = []
    for iteration in range(sweep_config["iterations"]):
        for database in sweep_config["databases"]:
            for locking_mode in sweep_config["locking_modes"]:
                for journal_mode in sweep_config["journal_modes"]:
                    for cache_size in sweep_config["cache_sizes"]:
                        for clients in sweep_config["num_clients"]:
                            for duration in sweep_config["durations"]:
                                for read_weight in sweep_config["read_weights"]:
                                    experiment_id = "%016x" % random.getrandbits(64)
                                    config = {
                                        "database": database["path"],
                                        "vfs": database["vfs"],
                                        "journal_mode": journal_mode,
                                        "locking_mode": locking_mode,
                                        "cache_size": cache_size }
                                    with open("tmp-config", "w") as f:
                                        f.write("# Auto-generated SQLite configuration file\n")
                                        f.write("[sqlite]\n\n")
                                        for k, v in config.items():
                                            f.write("%s = %s\n" % (k, str(v)))
                                    # additional configuration information
                                    config["clients"] = clients
                                    config["duration"] = duration
                                    config["experiment_id"] = experiment_id
                                    config["iteration"] = iteration
                                    config["read_weight"] = read_weight
                                    print("executing ", config)
                                    init_location(database["path"], database["vfs"], database["alt_path"] if "alt_path" in database else None)
                                    result_file = "res-%s.json" % experiment_id
                                    res = run_test("tmp-config", clients, duration, read_weight, result_file)
                                    with open(result_file) as f:
                                        result_data = json.load(f)
                                    json.dump({ "config" : config, "results": result_data }, results_f)
                                    results_f.write("\n")
                                    results_f.flush()
                                    os.remove(result_file)
