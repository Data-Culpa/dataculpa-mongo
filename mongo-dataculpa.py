#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# mongo-dataculpa.py
# MongoDB Data Culpa Connector
#
# Copyright © 2019-2020 Data Culpa, Inc. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#


# This reads from a database and sends new records to Validator, giving us a database-to-pipeline transform
# There are a few limitations:
# 1. we assume old records don't change for now; we don't go back and resample an old record.
#    there are a few ways to workaround this later.
# 2. it would be nice to plugin to the Mongo streaming API.
# 3. this assumes instantiation from cron or something similar; we don't provide job control around it.
# 

import argparse
import json
import os
import sys
import yaml

import decimal
import pymongo
import bson

from datetime import datetime

from dataculpa import DataCulpaValidator

from pymongo import MongoClient, DESCENDING

DEBUG = False


class MongoJSONEncoder(json.JSONEncoder):
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bson.ObjectId):
            return str(o)
        #        if isinstance(o, (datetime, date)):
        #            return iso.datetime_isoformat(o)

        if isinstance(o, decimal.Decimal):
            return f'{o.normalize():f}'  # using normalize() gets rid of trailing 0s, using ':f' prevents scientific notation

        return json.JSONEncoder.default(self, o)

class DbConfig:
    def __init__(self): #, db, host, port, user, password):
        self._d = { 'mode': 'database',
                    'dataculpa_controller':
                        {
                            'host': 'dataculpa-api',
                            'port': 7777,
                            #'url': 'http://192.168.1.13:7777',
                            'api-secret': 'set in .env file $DC_CONTROLLER_SECRET; not here',
                        },
                    'db_server': {
                        'host': 'localhost',
                        'dbname': 'dataculpa',
                        #'port': '27017',
                        'user': 'dataculpa',
                        'password': 'set in .env file $DB_PASSWORD; not here',
                        # databases, collections, etc...
                        #'collection_config':
                        #    [
                        #        { 'example_name': { 'enabled': False, 'id_field': '(infer)' }}
                        #    ]
                    },
                    'behavior':
                        {
                            'new_collections': 'traverse', # or ignore
                        },
                  }

    def save(self, fname):
        if os.path.exists(fname):
            print("%s exists already; rename it before creating a new example config." % fname)
            os._exit(1)
            return

        f = open(fname, 'w')
        yaml.safe_dump(self._d, f, default_flow_style=False)
        f.close()
        return

    def load(self, fname):
        with open(fname, "r") as f:
            #print(f)
            self._d = yaml.load(f, yaml.SafeLoader)
            #print(self._d)
        return

    def _get_db(self, field, default_value=None):
        d = self._d.get('db_server')
        return d.get(field, default_value)

    def generate_db_id_str(self):
        return "db_type:%s,host:%s:%s,name:%s" % ("mongo", self._get_db('host'), self._get_db('port'), self._get_db('dbname'))

    def get_db_mongo(self):
        return (self._get_db('host'),
                self._get_db('port', 27017),
                self._get_db('dbname'),
                self._get_db('user'),
                self._get_db('password'))

    def get_controller_config(self):
        return self._d.get('dataculpa_controller')

    def connect_controller(self, pipeline_name):
        cc = self.get_controller_config()
        host = cc.get('host')
        port = cc.get('port')
        v = DataCulpaValidator(pipeline_name,
                               protocol=DataCulpaValidator.HTTP,
                               dc_host=host,
                               dc_port=port)
        return v

    def get_db_table_config(self):
        return self._get_db('table_config')

    def get_table_config_for_table_name(self, name):
        for entry in self.get_db_table_config():
            if entry.get('name', '') == name:
                #return entry.get('enabled')
                return entry
        # endfor
        return None

    def get_behavior(self):
        return self._d.get('behavior')

    def get_traverse_new_tables(self):
        b = self.get_behavior()
        if b.get('new_collections', 'traverse') == 'traverse':
            return True
        return False

    def do_connect(self):
        (self._mongo_client, self._mongo_db) = self.get_mongo_connection()
        return

    def do_fetch_live_table_list(self):
        live_tables = []

        for coll in self._mongo_db.list_collection_names():
            live_tables.append(coll)

        return live_tables

    def do_fetch_data(self, table_name, maxCount=1000):
        pr = []
        result = self._mongo_db[table_name].find().sort("_id", DESCENDING).limit(maxCount)
        for r in result:
            pr.append(r)
            #print(r)
        return pr

    def get_mongo_connection(self):
        # mongodb
        # https://api.mongodb.com/python/current/examples/authentication.html
        # Some unsupported-by-us mechanisms documented at the above url.
        (host, port, dbname, user, password) = self.get_db_mongo()

        client = MongoClient(host, port)
        db = client[dbname]
        return (client, db)

    def test_connect_db(self):
        known_tables = []

        # mongodb
        (client, db) = self.get_mongo_connection()

        # print collections
        for coll in db.list_collection_names():
            known_tables.append(coll)

        tc = self.get_db_table_config()

        for entry in tc:
            assert entry.get('name') is not None
            assert entry.get('enabled') is not None

            table_name = entry.get('name')
            table_enabled = entry.get('enabled')

            print("%s -> %s" % (table_name, table_enabled))
        # endfor

        print(known_tables)

        return


def do_initdb():
    config = DbConfig()
    config.save("example.yaml")
    return

def do_test_config(fname):
    if not os.path.exists(fname):
        print("%s does NOT exist!" % fname)
        os._exit(1)
        return

    # load the config
    config = DbConfig()
    d = config.load(fname)

    # can we connect to the db?
    config.test_connect_db()

    # FIXME: can we ping the controller?
    return config

def do_sync_config(fname):
    print("Not yet implemented.")

    # so we run the test config
    config = do_test_config(fname)

    # ok, connect to the db again and load up all the table info.
    # for any table not found in the config, make a new entry showing that it is disabled.
    # for any table in the config but missing from the db, make a note.
    # pyyaml doesn't let us inject comments -- I noticed some other yaml packages for python do, but hesisate a bit to bring in a bunch of random things.


    return


def do_run(fname):
    print("do_run")

    # load the config
    config = DbConfig()
    d = config.load(fname)

    db_id_str = config.generate_db_id_str()

    # connect to the db.
    # if we can't connect, log an erto the cache.
    config.do_connect() # FIXME: handle errors.

    #traverse_new_tables = config.get_traverse_new_tables()

    # we need to keep some metadata of where we were at -- some state.
    #

    # log that we connected.
    #mc.write_run_status(db_id_str, "connected; will %straverse new tables" % ("" if traverse_new_tables else "NOT "))

    # get the job status for this identifier... see if we have something running...
    # host hash? some kind of local identity?  don't over think it for now.

    #table_config = config.get_db_table_config()

    # query the list of tables that are live.
    live_tables = config.do_fetch_live_table_list()

    for t in live_tables:
        pipeline_name = "database-%s-%s" % (db_id_str, t)
        # connect to the cache
        dc = config.connect_controller(pipeline_name)
        assert dc is not None

        table_config = config.get_table_config_for_table_name(t)
        scan_this_table = True

        if table_config is None:
            scan_this_table = config.get_traverse_new_tables()
            print("unconfigured table %s; default scan behavior is set to %s" % (t, scan_this_table))
        else:
            scan_this_table = table_config.get('enabled')
            if scan_this_table is not None:
                if scan_this_table:
                    print("table %s is explicitly disabled in config file" % (t))
                else:
                    print("table %s is explicitly enabled in config file" % (t))
                # endif
            else:
                # set to the default...
                print("table %s is not disabled; so we will scan it." % (t))
                # FIXME: We don't seem to have a default global
                scan_this_table = True
        # endif

        if not scan_this_table:
            continue
        # endif

        print("scanning table %s -- will collect newest 1000 records" % t)

        # figure out what's new...query and queue to validator.

        # get the most recent records and post them
        recordList = config.do_fetch_data(t, 1000);

        for r in recordList:
            # (dc_queueid, dc_queue_count, dc_queue_age)
            dc.queue_record(r, jsonEncoder=MongoJSONEncoder)
        #assert dc_queueid is not None, "got None for queue_id!"
        #print(dc_queueid, dc_queue_count, dc_queue_age)
        #    time.sleep(1)

        (dc_queueid, server_result) = dc.queue_commit()
        print("server_result: __%s__" % server_result)
    # endfor

    # walk the tables -- where we were
    return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--initdb",
                    help="one time use to generate a new db yaml file (prompts for inputs)",
                    action='store_true')
    ap.add_argument("--test-config",
                    help="takes a yaml config file and will run through the connections and verify things work")
    ap.add_argument("--sync-config",
                    help="Given a config file with working database creds, will update the config file to include an entry for all the collections found (with any new ones disabled). Run this to look for errors in the collection configuration, new collections that are unconfigured, etc. Returns 0 if everything lines up.")
    ap.add_argument("--run",
                    help="Run the db scan described in the specified yaml file")

    ap.add_argument("--debug",
                    help="print logs to the console",
                    action='store_true')

    args = ap.parse_args()

    global DEBUG
    if args.debug:
        DEBUG = True

    if args.initdb:
        do_initdb()
        return
    elif args.test_config:
        do_test_config(args.test_config)
        return
    elif args.sync_config:
        do_sync_config(args.sync_config)
        return
    elif args.run:
        do_run(args.run)
        return

    print("Try --help")
    os._exit(2)
    return


if __name__ == "__main__":
    main()
