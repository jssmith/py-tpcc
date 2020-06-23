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

import sys
import multiprocessing
import time
import random
import traceback
import logging
from datetime import datetime
from pprint import pprint,pformat

import constants
from util import *


class Executor:
    
    def __init__(self, driver, scaleParameters, stop_on_error = False, weights=None, cffs_ctl=None):
        self.driver = driver
        self.scaleParameters = scaleParameters
        self.stop_on_error = stop_on_error
        self.cffs_ctl = cffs_ctl

        if not weights:
            self.weights = {
                constants.TransactionTypes.STOCK_LEVEL: 4,
                constants.TransactionTypes.DELIVERY: 4,
                constants.TransactionTypes.ORDER_STATUS: 4,
                constants.TransactionTypes.PAYMENT: 43,
                constants.TransactionTypes.NEW_ORDER: 45
            }
        else:
            self.weights = weights
        sumw = 0
        self.txn_select = []
        for txn, w in self.weights.items():
            sumw += w
            self.txn_select.append([sumw, txn])
        self.sumw = sumw
        self.txn_params = {
            constants.TransactionTypes.STOCK_LEVEL: self.generateStockLevelParams,
            constants.TransactionTypes.DELIVERY: self.generateDeliveryParams,
            constants.TransactionTypes.ORDER_STATUS: self.generateOrderStatusParams,
            constants.TransactionTypes.PAYMENT: self.generatePaymentParams,
            constants.TransactionTypes.NEW_ORDER: self.generateNewOrderParams
        }

    ## DEF
    
    def execute(self, duration, record_detail):
        r = results.Results(record_detail)
        assert r
        logging.info("Executing benchmark for %d seconds" % duration)
        start = r.startBenchmark()
        debug = logging.getLogger().isEnabledFor(logging.DEBUG)

        while (time.time() - start) <= duration:
            time.sleep(0.01)
            txn, params = self.doOne()
            txn_id = r.startTransaction(txn)
            
            if debug: logging.debug("Executing '%s' transaction" % txn)
            try:
                try_query = True
                retry_ct = 0
                while try_query and (time.time() - start) <= duration:
                    try:
                        if self.cffs_ctl:
                            self.cffs_ctl.begin()
                            try:
                                self.driver.setup()
                                val = self.driver.executeTransaction(txn, params)
                            except Exception as ex:
                                print('have exception', ex)
                                self.driver.abort()
                                self.cffs_ctl.abort()
                                raise ex
                            self.cffs_ctl.commit()
                        else:
                            self.driver.setup()
                            val = self.driver.executeTransaction(txn, params)
                        try_query = False
                    except Exception as ex:
                        print(ex)
                        retry_ct += 1
                        if retry_ct >= 3:
                            print('retry ct exceeded')
                            raise ex
                        else:
                            time.sleep(0.001 * retry_ct)
                        # if retry_ct >= 20:
                        #     print("abort transaction")
                        #     raise ex
                        # if retry_ct > 3:
                        #     time.sleep(0.01 * retry_ct * retry_ct)
                if try_query:
                    r.abortTransaction(txn_id)
                    continue
            except KeyboardInterrupt:
                return -1
            except (Exception, AssertionError) as ex:
                logging.warn("Failed to execute Transaction '%s': %s" % (txn, ex))
                if debug: traceback.print_exc(file=sys.stdout)
                if self.stop_on_error: raise
                r.abortTransaction(txn_id)
                continue

            #if debug: logging.debug("%s\nParameters:\n%s\nResult:\n%s" % (txn, pformat(params), pformat(val)))
            
            r.stopTransaction(txn_id)
        ## WHILE
            
        r.stopBenchmark()
        return (r)
    ## DEF
    
    def doOne(self):
        """Selects and executes a transaction at random. The number of new order transactions executed per minute is the official "tpmC" metric. See TPC-C 5.4.2 (page 71)."""
        
        ## This is not strictly accurate: The requirement is for certain
        ## *minimum* percentages to be maintained. This is close to the right
        ## thing, but not precisely correct. See TPC-C 5.2.4 (page 68).
        x = rand.number(0, self.sumw - 1)
        txn = next(t[1] for t in self.txn_select if x < t[0])
        params = self.txn_params[txn]()
        
        return (txn, params)
    ## DEF

    ## ----------------------------------------------
    ## generateDeliveryParams
    ## ----------------------------------------------
    def generateDeliveryParams(self):
        """Return parameters for DELIVERY"""
        w_id = self.makeWarehouseId()
        o_carrier_id = rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
        ol_delivery_d = datetime.now()
        return makeParameterDict(locals(), "w_id", "o_carrier_id", "ol_delivery_d")
    ## DEF

    ## ----------------------------------------------
    ## generateNewOrderParams
    ## ----------------------------------------------
    def generateNewOrderParams(self):
        """Return parameters for NEW_ORDER"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_id = self.makeCustomerId()
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
        o_entry_d = datetime.now()

        ## 1% of transactions roll back
        rollback = False # FIXME rand.number(1, 100) == 1

        i_ids = [ ]
        i_w_ids = [ ]
        i_qtys = [ ]
        for i in range(0, ol_cnt):
            if rollback and i + 1 == ol_cnt:
                i_ids.append(self.scaleParameters.items + 1)
            else:
                i_ids.append(self.makeItemId())

            ## 1% of items are from a remote warehouse
            remote = (rand.number(1, 100) == 1)
            if self.scaleParameters.warehouses > 1 and remote:
                i_w_ids.append(rand.numberExcluding(self.scaleParameters.starting_warehouse, self.scaleParameters.ending_warehouse, w_id))
            else:
                i_w_ids.append(w_id)

            i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))
        ## FOR

        return makeParameterDict(locals(), "w_id", "d_id", "c_id", "o_entry_d", "i_ids", "i_w_ids", "i_qtys")
    ## DEF

    ## ----------------------------------------------
    ## generateOrderStatusParams
    ## ----------------------------------------------
    def generateOrderStatusParams(self):
        """Return parameters for ORDER_STATUS"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_last = None
        c_id = None
        
        ## 60%: order status by last name
        if rand.number(1, 100) <= 60:
            c_last = rand.makeRandomLastName(self.scaleParameters.customersPerDistrict)

        ## 40%: order status by id
        else:
            c_id = self.makeCustomerId()
            
        return makeParameterDict(locals(), "w_id", "d_id", "c_id", "c_last")
    ## DEF

    ## ----------------------------------------------
    ## generatePaymentParams
    ## ----------------------------------------------
    def generatePaymentParams(self):
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)
        y = rand.number(1, 100)

        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        c_w_id = None
        c_d_id = None
        c_id = None
        c_last = None
        h_amount = rand.fixedPoint(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT)
        h_date = datetime.now()

        ## 85%: paying through own warehouse (or there is only 1 warehouse)
        if self.scaleParameters.warehouses == 1 or x <= 85:
            c_w_id = w_id
            c_d_id = d_id
        ## 15%: paying through another warehouse:
        else:
            ## select in range [1, num_warehouses] excluding w_id
            c_w_id = rand.numberExcluding(self.scaleParameters.starting_warehouse, self.scaleParameters.ending_warehouse, w_id)
            assert c_w_id != w_id
            c_d_id = self.makeDistrictId()

        ## 60%: payment by last name
        if y <= 60:
            c_last = rand.makeRandomLastName(self.scaleParameters.customersPerDistrict)
        ## 40%: payment by id
        else:
            assert y > 60
            c_id = self.makeCustomerId()

        return makeParameterDict(locals(), "w_id", "d_id", "h_amount", "c_w_id", "c_d_id", "c_id", "c_last", "h_date")
    ## DEF

    ## ----------------------------------------------
    ## generateStockLevelParams
    ## ----------------------------------------------
    def generateStockLevelParams(self):
        """Returns parameters for STOCK_LEVEL"""
        w_id = self.makeWarehouseId()
        d_id = self.makeDistrictId()
        threshold = rand.number(constants.MIN_STOCK_LEVEL_THRESHOLD, constants.MAX_STOCK_LEVEL_THRESHOLD)
        return makeParameterDict(locals(), "w_id", "d_id", "threshold")
    ## DEF

    def makeWarehouseId(self):
        w_id = rand.number(self.scaleParameters.starting_warehouse, self.scaleParameters.ending_warehouse)
        assert(w_id >= self.scaleParameters.starting_warehouse), "Invalid W_ID: %d" % w_id
        assert(w_id <= self.scaleParameters.ending_warehouse), "Invalid W_ID: %d" % w_id
        return w_id
    ## DEF

    def makeDistrictId(self):
        return rand.number(1, self.scaleParameters.districtsPerWarehouse)
    ## DEF

    def makeCustomerId(self):
        return rand.NURand(1023, 1, self.scaleParameters.customersPerDistrict)
    ## DEF

    def makeItemId(self):
        return rand.NURand(8191, 1, self.scaleParameters.items)
    ## DEF
## CLASS

def makeParameterDict(values, *args):
    return dict(map(lambda x: (x, values[x]), args))
## DEF
