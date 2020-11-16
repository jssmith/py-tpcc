# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import logging
import time

class Results:
    
    def __init__(self, log_timing_details=False):
        self.log_timing_details = log_timing_details
        if self.log_timing_details:
            self.timing_details = []

        self.start = None
        self.stop = None
        self.txn_id = 0
        
        self.txn_counters = { }
        self.txn_times = { }
        self.txn_counters_aborted = { }
        self.txn_times_aborted = { }
        self.running = { }

        
    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start == None
        logging.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start
        
    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        logging.debug("Stopping benchmark statistics collection")
        self.stop = time.time()
        
    def startTransaction(self, txn):
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time())
        return id
        
    def hasTransaction(self, id):
        return id in self.running
        
    def abortTransaction(self, id):
        """Abort a transaction and discard its times"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]

        txn_end = time.time()
        duration = txn_end - txn_start
        total_time = self.txn_times_aborted.get(txn_name, 0)
        self.txn_times_aborted[txn_name] = total_time + duration

        total_cnt = self.txn_counters_aborted.get(txn_name, 0)
        self.txn_counters_aborted[txn_name] = total_cnt + 1

        if self.log_timing_details:
            txn_end = time.time()
            self.timing_details.append({
                "txn_name": txn_name,
                "start_time": txn_start,
                "end_time": txn_end,
                "success": False
            })
        
    def stopTransaction(self, id):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]
        
        txn_end = time.time()
        duration = txn_end - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration
        
        total_cnt = self.txn_counters.get(txn_name, 0)
        self.txn_counters[txn_name] = total_cnt + 1

        if self.log_timing_details:
            self.timing_details.append({
                "txn_name": txn_name,
                "start_time": txn_start,
                "end_time": txn_end,
                "success": True
            })

    def append(self, r):
        for txn_name in r.txn_counters.keys():
            orig_cnt = self.txn_counters.get(txn_name, 0)
            orig_time = self.txn_times.get(txn_name, 0)

            self.txn_counters[txn_name] = orig_cnt + r.txn_counters[txn_name]
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            #logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
        ## HACK
        if not self.start:
            self.start = r.start
        elif r.start:
            self.start = min(self.start, r.start)
        if not self.stop:
            self.stop = r.stop
        elif r.stop:
            self.stop = max(self.stop, r.stop)

    def __str__(self):
        return self.show()
        
    def data(self, load_time = None):
        if self.start == None:
            return "Benchmark not started"
        if self.stop == None:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start
        res = {}
        if load_time:
            res["LoadTime"] = load_time

        total_time = 0
        total_cnt = 0
        res["Txns"] = []
        for txn in sorted(self.txn_counters.keys()):
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            rate = txn_cnt / txn_time
            res["Txns"].append({ "Txn": txn, "Ct": txn_cnt, "Time": txn_time })
            total_time += txn_time
            total_cnt += txn_cnt

        total_aborted_time = 0
        total_aborted_cnt = 0
        res["TxnsAborted"] = []
        for txn in sorted(self.txn_counters_aborted.keys()):
            txn_aborted_time = self.txn_times_aborted[txn]
            txn_aborted_cnt = self.txn_counters_aborted[txn]
            rate = txn_aborted_cnt / txn_aborted_time
            res["TxnsAborted"].append({ "Txn": txn, "Ct": txn_aborted_cnt, "Time": txn_aborted_time })
            total_aborted_time += txn_aborted_time
            total_aborted_cnt += txn_aborted_cnt

        res["TxnsTotal"] = { "Ct": total_cnt, "Time": total_time, "Duration": duration }
        res["TxnsAbortedTotal"] = { "Ct": total_aborted_cnt, "Time": total_aborted_time, "Duration": duration }
        if self.log_timing_details:
            res["TxnsDetail"] = self.timing_details
        return res


    def show(self, load_time = None):
        data = self.data()

        col_width = 16
        total_width = (col_width*4)+2
        f = "\n  " + (("%-" + str(col_width) + "s")*4)
        line = "-"*total_width

        ret = u"" + "="*total_width + "\n"
        if "LoadTime" in data:
            ret += "Data Loading Time: %d seconds\n\n" % (data["LoadTime"])

        duration = data["TxnsTotal"]["Duration"]
        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Executed", u"Time (us)", "Rate")

        total_time = 0
        total_cnt = 0
        for txn_data in data["Txns"]:
            txn = txn_data["Txn"]
            txn_time = txn_data["Time"]
            txn_cnt = txn_data["Ct"]
            rate = u"%.02f txn/s" % ((txn_cnt / txn_time))
            ret += f % (txn, str(txn_cnt), str(int(txn_time * 1000000)), rate)
        total_cnt = data["TxnsTotal"]["Ct"]
        total_time = data["TxnsTotal"]["Time"]
        total_rate = "%.02f txn/s" % ((total_cnt / duration))
        concurrency = "%.02f" % (total_time / duration)
        ret += f % ("TOTAL", str(total_cnt), str(int(duration * 1000000)), total_rate)

        return ret
## CLASS
