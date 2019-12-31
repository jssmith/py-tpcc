import fcntl
import os

class Control:
    def __init__(self, cffs_path):
        self.cffs_fd = os.open(cffs_path, os.O_RDWR)

    def begin(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000001)

    def commit(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000002)

    def abort(self):
        fcntl.ioctl(self.cffs_fd, 0xCF000003)
