import argparse
import json
import socket

from datetime import datetime

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class SqliteProxyConnection(object):
    def __init__(self):
        self.host = host
        self.port = port

    class SqliteProxyCursor(object):
        def __init__(self, proxy_connection):
            self.proxy_connection = proxy_connection

        def execute(self, statement, statement_args=[]):
            rpc_req = {
                "command": "execute",
                "args": {
                    "statement": statement,
                    "statement_args": statement_args
                }
            }
            res = self.proxy_connection.rpc(rpc_req)
            if "success" in res and res["success"]:
                self.rows = res["rows"]
                self.n_rows = len(self.rows)
                self.cur_row = 0
                return True
            else:
                self.rows = None
                self.n_rows = 0
                self.cur_row = 0
                return False

        def fetchone(self):
            if self.cur_row < self.n_rows:
                rv = self.rows[self.cur_row]
                self.cur_row += 1
                return rv
            else:
                return None

        def fetchall(self):
            rv = self.rows[self.cur_row:]
            self.cur_row = self.n_rows
            return rv


    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def close(self):
        self.socket.close()

    def rpc(self, data):
        self.socket.sendall(json.dumps(data, cls=DatetimeEncoder).encode("utf-8"))
        return json.loads(self.socket.recv(819200).decode("utf-8"))

    def commit(self):
        rpc_req = {
            "command": "commit",
            "args": { }
        }
        self.rpc(rpc_req)

    def cursor(self):
        return SqliteProxyConnection.SqliteProxyCursor(self)

def test_basic(c):
    c.execute("drop table if exists t1")
    c.execute("create table t1 (id int)")
    c.execute("insert into t1 (id) values(123)")
    c.execute("insert into t1 (id) values(?)", [124])
    c.execute("select count(*) from t1")
    print(c.fetchone()[0])
    c.execute("select * from t1")
    print(c.fetchall())
    conn.commit()

def test_timestamp(c):
    c.execute("drop table if exists t2")
    c.execute("create table t2 (ts TIMESTAMP)")
    c.execute("insert into t2 (ts) values(?)", [datetime.now()])
    conn.commit()
    c.execute("select count(*) from t2")
    print(c.fetchone()[0])
    c.execute("select * from t2")
    print(c.fetchall())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sqlite network service")
    parser.add_argument("--port", type=int, default=5478,
                         help="Port to listen on")
    args = vars(parser.parse_args())

    conn = SqliteProxyConnection("localhost", args["port"])
    c = conn.cursor()
    test_basic(c)
    test_timestamp(c)
