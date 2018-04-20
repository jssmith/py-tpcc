from . import abstractdriver
from . import sqlitedriver

import sqliteclient

class SqliteproxyDriver(sqlitedriver.SqliteDriver):
    DEFAULT_CONFIG = {
        "host": ("Host of the SQLite server", "localhost"),
        "port": ("Port of the SQLite server", "9845"),
    }

    def __init__(self, ddl):
        abstractdriver.AbstractDriver.__init__(self, "sqliteproxy", ddl)
        self.conn = None
        self.cursor = None

    def loadConfig(self, config):
        for key in SqliteproxyDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        self.conn = sqliteclient.SqliteProxyConnection(config["host"], int(config["port"]))
        self.cursor = self.conn.cursor()
