import argparse
import json
import logging
import multiprocessing
import socket
import sqlite3


class SQLiteServer(object):
    def __init__(self, hostname, port, database):
        self.hostname = hostname
        self.port = port
        self.database = database

    def handle(self, connection, client_addr):
        conn = sqlite3.connect(self.database)
        self.logger.info("connected to %s" % self.database)
        # print(">>>>>>>>>>>>>>>>connected to %s" % self.database)
        c = conn.cursor()
        try:
            while True:
                data = connection.recv(4096)
                # print("received \"%s\"" % data)
                if data:
                    rpc = json.loads(data)
                    # print(rpc)
                    try:
                        if rpc["command"] == "execute":
                            args = rpc["args"]
                            c.execute(args["statement"], args["statement_args"])
                            all_res = c.fetchall()
                            res = {
                                "success": True,
                                "rows": all_res
                            }
                        elif rpc["command"] == "commit":
                            conn.commit()
                            res = {
                                "success": True
                            }
                        else:
                            print("unkown command %s" % rpc["command"])
                            res = {
                                "success": False,
                                "error": "Unknown command"
                            }
                    except Exception as e:
                        res = {
                            "success": False,
                            "error": str(e)
                        }
                    # print('sending data back to the client')
                    # print(res)
                    connection.sendall(json.dumps(res).encode("utf-8"))
                else:
                    print("no more data from", client_addr)
                    break
        finally:
            print("closing down")
            conn.close()
            connection.close()


    def start(self):
        self.logger = logging.getLogger("server")
        self.logger.setLevel(logging.DEBUG)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.hostname, self.port))
        self.socket.listen(1)
        self.logger.info("listening on port %d" % self.port)
        print("listening on port %d..." % self.port)

        try:
            while True:
                conn, addr = self.socket.accept()
                self.logger.debug("Got connection")
                process = multiprocessing.Process(target=self.handle, args=(conn, addr))
                process.daemon = True
                process.start()
                self.logger.debug("Started process %r", process)
        finally:
            self.socket.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sqlite network service")
    parser.add_argument("--port", type=int, default=5478,
                        help="Port to listen on")
    parser.add_argument("--database", default=":memory:",
                        help="Path to database file")
    args = vars(parser.parse_args())

    logging.getLogger().setLevel(logging.DEBUG)
    port = args["port"]
    database = args["database"]
    s = SQLiteServer("0.0.0.0", port, database)
    s.start()
