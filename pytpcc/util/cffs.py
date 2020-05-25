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
            except ex:
                print(ex, file=sys.stderr)
                pass

    def begin(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000001)

    def commit(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000002)

    def abort(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000003)
