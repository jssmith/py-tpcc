# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
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

from __future__ import with_statement

import os
import sqlite3
import logging
#import commands
from pprint import pprint,pformat
import sys

import constants
from . import abstractdriver

SQL_STOCKCOUNT = """
    SELECT COUNT(DISTINCT(OL_I_ID)) FROM P%d.ORDER_LINE, P%d.STOCK
    WHERE OL_W_ID = ?
        AND OL_D_ID = ?
        AND OL_O_ID < ?
        AND OL_O_ID >= ?
        AND S_W_ID = ?
        AND S_I_ID = OL_I_ID
        AND S_QUANTITY < ?
"""

activated_partitions = set()
global_database = ""

def attach(c, p):
    global activated_partitions
    dbname = global_database % p
    if not p in activated_partitions:
        logging.debug("Attach to database %s" % dbname)
        c.execute("ATTACH DATABASE '%s' AS P%d" % (dbname, p))
        activated_partitions.add(p)
    else:
        logging.debug("Already attached to database %s" % dbname)

def execQuery(c, query, partitions, args):
    sql = query % partitions
    logging.debug("run query %s with args %s", sql, str(args))
    for p in partitions:
        attach(c, p)
    c.execute(sql, args)

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": lambda c, no_d_id, no_w_id: execQuery(c, "SELECT NO_O_ID FROM P%d.NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", (no_w_id,), [no_d_id, no_w_id]),
        "deleteNewOrder": lambda c, no_d_id, no_w_id, no_o_id: execQuery(c, "DELETE FROM P%d.NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", (no_w_id,), [no_d_id, no_w_id, no_o_id]),
        "getCId": lambda c, no_o_id, d_id, w_id: execQuery(c, "SELECT O_C_ID FROM P%d.ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", (w_id,), [no_o_id, d_id, w_id]),
        "updateOrders": lambda c, o_carrier_id, no_o_id, d_id, w_id: execQuery(c, "UPDATE P%d.ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", (w_id,), [o_carrier_id, no_o_id, d_id, w_id]),
        "updateOrderLine": lambda c, o_entry_d, no_o_id, d_id, w_id: execQuery(c, "UPDATE P%d.ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", (w_id,), [o_entry_d, no_o_id, d_id, w_id]),
        "sumOLAmount": lambda c, no_o_id, d_id, w_id: execQuery(c, "SELECT SUM(OL_AMOUNT) FROM P%d.ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", (w_id,), [no_o_id, d_id, w_id]),
        "updateCustomer": lambda c, ol_total, c_id, d_id, w_id: execQuery(c, "UPDATE P%d.CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", (w_id,), [ol_total, c_id, d_id, w_id]),
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": lambda c, w_id: execQuery(c, "SELECT W_TAX FROM P%d.WAREHOUSE WHERE W_ID = ?", (w_id,), [w_id]),
        "getDistrict": lambda c, d_id, w_id: execQuery(c, "SELECT D_TAX, D_NEXT_O_ID FROM P%d.DISTRICT WHERE D_ID = ? AND D_W_ID = ?", (w_id,), [d_id, w_id]),
        "incrementNextOrderId": lambda c, d_next_o_id, d_id, w_id: execQuery(c, "UPDATE P%d.DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", (w_id,), [d_next_o_id, d_id, w_id]),
        "getCustomer": lambda c, w_id, d_id, c_id: execQuery(c, "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM P%d.CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", (w_id,), [w_id, d_id, c_id]),
        "createOrder": lambda c, d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local: execQuery(c, "INSERT INTO P%d.ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (w_id,), [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local]),
        "createNewOrder": lambda c, o_id, d_id, w_id: execQuery(c, "INSERT INTO P%d.NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", (w_id,), [o_id, d_id, w_id]),
        "getItemInfo": lambda c, w_id, ol_i_id: execQuery(c, "SELECT I_PRICE, I_NAME, I_DATA FROM P%d.ITEM WHERE I_ID = ?", (w_id,), [ol_i_id]),
        "getStockInfo": lambda c, d_id, ol_i_id, ol_supply_w_id: execQuery(c, "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM P%%d.STOCK WHERE S_I_ID = ? AND S_W_ID = ?" % d_id, (ol_supply_w_id,), [ol_i_id, ol_supply_w_id]),
        "updateStock": lambda c, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id: execQuery(c, "UPDATE P%d.STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", (ol_supply_w_id,), [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id]),
        "createOrderLine": lambda c, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx: execQuery(c, "INSERT INTO P%d.ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (w_id,), [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx]),
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": lambda c, w_id, d_id, c_id: execQuery(c, "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM P%d.CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", (w_id,), [w_id, d_id, c_id]),
        "getCustomersByLastName": lambda c, w_id, d_id, c_last: execQuery(c, "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM P%d.CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", (w_id,), [w_id, d_id, c_last]),
        "getLastOrder": lambda c, w_id, d_id, c_id: execQuery(c, "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM P%d.ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", (w_id,), [w_id, d_id, c_id]),
        "getOrderLines": lambda c, w_id, d_id, o_id: execQuery(c, "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM P%d.ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", (w_id,), [w_id, d_id, o_id]),
    },
    
    "PAYMENT": {
        "getWarehouse": lambda c, w_id: execQuery(c, "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM P%d.WAREHOUSE WHERE W_ID = ?", (w_id,), [w_id]),
        "updateWarehouseBalance": lambda c, h_amount, w_id: execQuery(c, "UPDATE P%d.WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", (w_id,), [h_amount, w_id]),
        "getDistrict": lambda c, w_id, d_id: execQuery(c, "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM P%d.DISTRICT WHERE D_W_ID = ? AND D_ID = ?", (w_id,), [w_id, d_id]),
        "updateDistrictBalance": lambda c, h_amount, d_w_id, d_id: execQuery(c, "UPDATE P%d.DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?", (d_w_id,), [h_amount, d_w_id, d_id]),
        "getCustomerByCustomerId": lambda c, w_id, d_id, c_id: execQuery(c, "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM P%d.CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", (w_id,), [w_id, d_id, c_id]),
        "getCustomersByLastName": lambda c, w_id, d_id, c_last: execQuery(c, "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM P%d.CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", (w_id,), [w_id, d_id, c_last]),
        "updateBCCustomer": lambda c, c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id: execQuery(c, "UPDATE P%d.CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", (c_w_id,), [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id]),
        "updateGCCustomer": lambda c, c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id: execQuery(c, "UPDATE P%d.CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", (c_w_id,), [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id]),
        "insertHistory": lambda c, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data: execQuery(c, "INSERT INTO P%d.HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (h_w_id,), [h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data]),
    },
    
    "STOCK_LEVEL": {
        "getOId": lambda c, d_w_id, d_id: execQuery(c, "SELECT D_NEXT_O_ID FROM P%d.DISTRICT WHERE D_W_ID = ? AND D_ID = ?", (d_w_id,), [d_w_id, d_id]),
        "getStockCount": lambda c, ol_w_id, ol_d_id, ol_o_id_max, ol_o_id_min, s_w_id, s_quantity_max: execQuery(c, SQL_STOCKCOUNT, (ol_w_id, s_w_id), [ol_w_id, ol_d_id, ol_o_id_max, ol_o_id_min, s_w_id, s_quantity_max]),
    },
}


## ==============================================
## SqliteDriver
## ==============================================
class SqliteDriver(abstractdriver.AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("The path to the SQLite database", "/tmp/tpcc.db" ),
        "vfs": ("The SQLite VFS", "unix"),
        "journal_mode": ("The journal mode, e.g., wal, delete, etc.", "delete"),
        "locking_mode": ("The locking mode, either normal or exclusive", "normal"),
        "cache_size": ("The SQLite cache size in multiples of 1024 kb", 2000)
    }
    
    def __init__(self, ddl):
        super(SqliteDriver, self).__init__("sqlite", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return SqliteDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in SqliteDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        self.database = str(config["database"])
        global global_database
        global_database = self.database
        self.vfs = str(config["vfs"]).lower()
        self.journal_mode = str(config["journal_mode"]).lower()
        self.locking_mode = str(config["locking_mode"]).lower()
        self.cache_size = int(config["cache_size"])

        self.conn = sqlite3.connect(self.database)
        self.cursor = self.conn.cursor()

        try:
            self.cursor.execute("PRAGMA cache_size")
            current_cache_size = int(self.cursor.fetchone()[0])
            if self.cache_size != current_cache_size:
                self.cursor.execute("PRAGMA cache_size=-%d" % self.cache_size)
        except sqlite3.OperationalError as err:
            print(err, file=sys.stderr)

        try:
            self.cursor.execute("PRAGMA locking_mode")
            current_locking_mode = self.cursor.fetchone()[0]
            if self.locking_mode != current_locking_mode:
                self.cursor.execute("PRAGMA locking_mode=%s" % self.locking_mode)
        except sqlite3.OperationalError as err:
            print(err, file=sys.stderr)

        try:
            self.cursor.execute("PRAGMA journal_mode")
            current_journal_mode = self.cursor.fetchone()[0]
            if self.journal_mode != current_journal_mode:
                self.cursor.execute("PRAGMA journal_mode=%s" % self.journal_mode)
        except sqlite3.OperationalError as err:
            print(err, file=sys.stderr)

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        p = ["?"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (tableName, ",".join(p))
        self.cursor.executemany(sql, tuples)
        
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Commiting changes to database")
        self.conn.commit()

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        q = TXN_QUERIES["DELIVERY"]
        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            q["getNewOrder"](self.cursor, d_id, w_id)
            newOrder = self.cursor.fetchone()
            if newOrder == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrder) > 0
            no_o_id = newOrder[0]
            
            q["getCId"](self.cursor, no_o_id, d_id, w_id)
            c_id = self.cursor.fetchone()[0]
            
            q["sumOLAmount"](self.cursor, no_o_id, d_id, w_id)
            ol_total = self.cursor.fetchone()[0]

            q["deleteNewOrder"](self.cursor, d_id, w_id, no_o_id)
            q["updateOrders"](self.cursor, o_carrier_id, no_o_id, d_id, w_id)
            q["updateOrderLine"](self.cursor, ol_delivery_d, no_o_id, d_id, w_id)

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            q["updateCustomer"](self.cursor, ol_total, c_id, d_id, w_id)

            result.append((d_id, no_o_id))
        ## FOR

        self.conn.commit()
        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        need_partitions = set(i_w_ids)
        need_partitions.add(w_id)
        for p in need_partitions:
            attach(self.cursor, p)
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id
            q["getItemInfo"](self.cursor, w_id, i_ids[i])
            items.append(self.cursor.fetchone())
        assert len(items) == len(i_ids)
        
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        for item in items:
            if len(item) == 0:
                ## TODO Abort here!
                return
        ## FOR")
        
        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        q["getWarehouseTaxRate"](self.cursor, w_id)
        w_tax = self.cursor.fetchone()[0]
        
        q["getDistrict"](self.cursor, d_id, w_id)
        district_info = self.cursor.fetchone()
        d_tax = district_info[0]
        d_next_o_id = district_info[1]
        
        q["getCustomer"](self.cursor, w_id, d_id, c_id)
        customer_info = self.cursor.fetchone()
        c_discount = customer_info[0]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        q["incrementNextOrderId"](self.cursor, d_next_o_id + 1, d_id, w_id)
        q["createOrder"](self.cursor, d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local)
        q["createNewOrder"](self.cursor, d_next_o_id, d_id, w_id)

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo[1]
            i_data = itemInfo[2]
            i_price = itemInfo[0]

            q["getStockInfo"](self.cursor, d_id, ol_i_id, ol_supply_w_id)
            stockInfo = self.cursor.fetchone()
            if len(stockInfo) == 0:
                logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                continue
            s_quantity = stockInfo[0]
            s_ytd = stockInfo[2]
            s_order_cnt = stockInfo[3]
            s_remote_cnt = stockInfo[4]
            s_data = stockInfo[1]
            s_dist_xx = stockInfo[5] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1

            q["updateStock"](self.cursor, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id)

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            q["createOrderLine"](self.cursor, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx)

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Commit!
        self.conn.commit()

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        
        return [ customer_info, misc, item_data ]

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            q["getCustomerByCustomerId"](self.cursor, w_id, d_id, c_id)
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            q["getCustomersByLastName"](self.cursor, w_id, d_id, c_last)
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[int(index)]
            c_id = customer[0]
        assert len(customer) > 0
        assert c_id != None

        q["getLastOrder"](self.cursor, w_id, d_id, c_id)
        order = self.cursor.fetchone()
        if order:
            q["getOrderLines"](self.cursor, w_id, d_id, order[0])
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        self.conn.commit()
        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        if c_id != None:
            q["getCustomerByCustomerId"](self.cursor, w_id, d_id, c_id)
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            q["getCustomersByLastName"](self.cursor, w_id, d_id, c_last)
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[int(index)]
            c_id = customer[0]
        assert len(customer) > 0
        c_balance = customer[14] - h_amount
        c_ytd_payment = customer[15] + h_amount
        c_payment_cnt = customer[16] + 1
        c_data = customer[17]

        q["getWarehouse"](self.cursor, w_id)
        warehouse = self.cursor.fetchone()
        
        q["getDistrict"](self.cursor, w_id, d_id)
        district = self.cursor.fetchone()
        
        q["updateWarehouseBalance"](self.cursor, h_amount, w_id)
        q["updateDistrictBalance"](self.cursor, h_amount, w_id, d_id)

        # Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            q["updateBCCustomer"](self.cursor, c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id)
        else:
            c_data = ""
            q["updateGCCustomer"](self.cursor, c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id)

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse[0], district[0])
        # Create the history record
        q["insertHistory"](self.cursor, c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data)

        self.conn.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        q["getOId"](self.cursor, w_id, d_id)
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        q["getStockCount"](self.cursor, w_id, d_id, o_id, (o_id - 20), w_id, threshold)
        result = self.cursor.fetchone()
        
        self.conn.commit()
        
        return int(result[0])
        
## CLASS
