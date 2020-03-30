#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
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
import os
import json
import string
import datetime
import logging
import re
import argparse
import glob
import time
import multiprocessing
from configparser import ConfigParser
from pprint import pprint,pformat

import constants

from util import *
from runtime import *
import drivers

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    
## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass
## DEF

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = [ ]
    for f in map(lambda x: os.path.basename(x).replace("driver.py", ""), glob.glob("./drivers/*driver.py")):
        if f != "abstract": drivers.append(f)
    return (drivers)
## DEF

## ==============================================
## startLoading
## ==============================================
def startLoading(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    
    # Split the warehouses into chunks
    w_ids = map(lambda x: [ ], range(args['clients']))
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % args['clients']
        w_ids[idx].append(w_id)
    ## FOR
    
    loader_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(loaderFunc, (driverClass, scaleParameters, args, config, w_ids[i], True))
        loader_results.append(r)
    ## FOR
    
    pool.close()
    logging.debug("Waiting for %d loaders to finish" % args['clients'])
    pool.join()
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s [warehouses=%d]" % (driver, len(w_ids)))
    
    config['load'] = True
    config['execute'] = False
    config['reset'] = False
    driver.loadConfig(config)
   
    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()   
    except KeyboardInterrupt:
            return -1
    except (Exception, AssertionError) as ex:
        logging.warn("Failed to load data: %s" % (ex))
        #if debug:
        traceback.print_exc(file=sys.stdout)
        raise
        
## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    
    # remove non-serializable arguments
    worker_args = args.copy()
    if 'json_output' in worker_args:
        del worker_args['json_output']
    if 'config' in worker_args:
        del worker_args['config']

    worker_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, worker_args, config, debug,))
        worker_results.append(r)
    ## FOR
    pool.close()
    pool.join()
    
    total_results = results.Results()
    for asyncr in worker_results:
        asyncr.wait()
        r = asyncr.get()
        assert r != None, "No results object returned!"
        if type(r) == int and r == -1: sys.exit(1)
        total_results.append(r)
    ## FOR
    
    return (total_results)
## DEF

## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s" % driver)
    
    cffs_ctl = None
    if args['cffs_mount']:
        cffs_ctl = cffs.Control(args['cffs_mount'])
        cffs_ctl.begin()

    config['execute'] = True
    config['reset'] = False
    driver.loadConfig(config)

    e = executor.Executor(config, driver, scaleParameters, stop_on_error=args['stop_on_error'], weights=config['txn_weights'], cffs_ctl=cffs_ctl)
    driver.executeStart()
    results = e.execute(args['duration'], args['timing_details'])
    driver.executeFinish()
    
    return results
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=argparse.FileType('r'),
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--frac-read', default=None, type=float,
                         help='fraction of reads')
    aparser.add_argument('--cffs-mount', default=None,
                         help='mount point for CFFS (enables transactions)')
    aparser.add_argument('--ddl', default=os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql")),
                         help='Path to the TPC-C DDL SQL file')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')
    aparser.add_argument('--json-output', type=argparse.FileType('a'),
                         help='append json-formatted performance numbers to file')
    aparser.add_argument('--timing-details', action='store_true',
                         help='Capture timing details on each query')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
        
    ## Create a handle to the target client driver
    driverClass = createDriverClass(args['system'])
    assert driverClass != None, "Failed to find '%s' class" % args['system']
    driver = driverClass(args['ddl'])
    assert driver != None, "Failed to create '%s' driver" % args['system']
    if args['print_config']:
        config = driver.makeDefaultConfig()
        print(driver.formatConfig(config))
        print()
        sys.exit(0)

    ## Load Configuration file
    if args['config']:
        logging.debug("Loading configuration file '%s'" % args['config'])
        cparser = ConfigParser()
        cparser.read(os.path.realpath(args['config'].name))
        config = dict(cparser.items(args['system']))
    else:
        logging.debug("Using default configuration for %s" % args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
    config['load'] = False
    config['execute'] = False
    config['reset'] = args['reset']
    if config['reset']: logging.info("Reseting database")
    config['txn_weights'] = None
    if args['frac_read']:
        f = args['frac_read']
        if f < 0.0 or f > 1.0:
            print("read fraction must be in range [0,1]")
            sys.exit(1)
        config['txn_weights'] = {
            constants.TransactionTypes.STOCK_LEVEL: int(500 * f),
            constants.TransactionTypes.DELIVERY: int(44 * (1 - f)),
            constants.TransactionTypes.ORDER_STATUS: int(500 * f),
            constants.TransactionTypes.PAYMENT: int(467 * (1 - f)),
            constants.TransactionTypes.NEW_ORDER: int(489 * (1 - f))
        }

    cffs_ctl = None
    txn_stats_file = None
    if args['cffs_mount']:
        if 'CFFS_STAT' in os.environ:
            txn_stats_file = "%s-%d.txn" % (os.environ['CFFS_STAT'], os.getpid())
        cffs_ctl = cffs.Control(args['cffs_mount'])
        while True:
            cffs_ctl.begin()
            try:
                driver.loadConfig(config)
                break
            except Exception as ex:
                print("db connect failed:", ex)
                if str(ex) == "database disk image is malformed":
                    sys.exit(1)
                self.cffs_ctl.abort()
    else:
        driver.loadConfig(config)

    logging.info("Initializing TPC-C benchmark using %s" % driver)

    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    nurand = rand.setNURand(nurand.makeForLoad())
    if args['debug']: logging.debug("Scale Parameters:\n%s" % scaleParameters)
    
    ## DATA LOADER!!!
    load_time = None
    if not args['no_load']:
        logging.info("Loading TPC-C benchmark data using %s" % (driver))
        load_start = time.time()
        if args['clients'] == 1:
            l = loader.Loader(driver, scaleParameters, range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1), True)
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driverClass, scaleParameters, args, config)
        load_time = time.time() - load_start
    ## IF
    
    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        if txn_stats_file:
            txn_stats = cffs.Stats()
        if args['clients'] == 1:
            e = executor.Executor(config, driver, scaleParameters, stop_on_error=args['stop_on_error'], weights=config['txn_weights'], cffs_ctl=cffs_ctl)
            driver.executeStart()
            results = e.execute(args['duration'], args['timing_details'])
            driver.executeFinish()
        else:
            results = startExecution(driverClass, scaleParameters, args, config)
        assert results
        if args['json_output']:
            json.dump(results.data(load_time), args['json_output'])
            args['json_output'].write("\n")
        if txn_stats_file:
            with open(txn_stats_file, "w") as f:
                for k, vals in txn_stats.finish().items():
                    f.write("%d,%s\n" % (k, ",".join([str(x) for x in vals])))
            print("stats written to %s" % txn_stats_file)
        print(results.show(load_time))
    ## IF

    if cffs_ctl:
        cffs_ctl.commit()
    
## MAIN
