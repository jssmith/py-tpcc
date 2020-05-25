import fcntl
import http.client
import os

from collections import OrderedDict

class Control:
    def __init__(self, cffs_path):
        while True:
            try:
                self.cffs_fd = os.open(cffs_path, os.O_RDWR)
                break
            except Exception as ex:
                print(ex, file=sys.stderr)
                pass

    def begin(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000001)

    def commit(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000002)

    def abort(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000003)

class Stats:
    def __init__(self):
        self.inital_cts = Stats.fetch_counts()

    def finish(self):
        final_cts = Stats.fetch_counts()
        deltas = OrderedDict()
        for k in self.inital_cts.keys():
            final_row = final_cts[k]
            row = []
            for i, initial_ct in enumerate(self.inital_cts[k]):
                 row.append(final_row[i] - initial_ct)
            deltas[k] = row
        return deltas

    @staticmethod
    def fetch_counts():
        c = http.client.HTTPConnection("localhost", 10023, timeout=1)
        c.request("GET", "/")
        resp = c.getresponse()
        cts = OrderedDict()
        for line in resp.read().decode("utf-8").splitlines():
            parts = list([int(x) for x in line.split(",")])
            stat_id = parts[0]
            stats = parts[1:]
            cts[stat_id] = stats
        return cts
