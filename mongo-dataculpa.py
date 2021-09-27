#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# mongo-dataculpa.py
# MongoDB Data Culpa Connector
#
# Copyright © 2019-2021 Data Culpa, Inc. All rights reserved.
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
import dotenv
import json
import logging
import os
import pickle
import sys
import sqlite3
import time
import yaml

import decimal
import pymongo
import bson

from datetime import datetime, timedelta
from dateutil.parser import parse as DateUtilParse

from dataculpa import DataCulpaValidator

from pymongo import MongoClient, ASCENDING

DEBUG = False

def FatalError(message, rc=2):
    sys.stderr.write(message)
    sys.stderr.write("\n")
    sys.stderr.flush()
    os._exit(rc)
    return



if sys.version_info[0] < 3:
    raise Exception("This code requires python 3")

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bson.ObjectId):
            return str(o)
        #        if isinstance(o, (datetime, date)):
        #            return iso.datetime_isoformat(o)

        if isinstance(o, decimal.Decimal):
            return f'{o.normalize():f}'  # using normalize() gets rid of trailing 0s, using ':f' prevents scientific notation

        return json.JSONEncoder.default(self, o)

class Config:
    def __init__(self): #, db, host, port, user, password):
        self._d = { 
                    'dataculpa_controller': {
                            'host': 'localhost',
                            'port': 7777,
                            'api_key': '[required] fill me in'
                    },
                    'configuration': {
                        'host': '[required] localhost',
                        'user': '[optional] dataculpa',
                        'port': '[optional] 27017',
                        'dbname': '[required] name',
                        'collections': []

                    },
                    'dataculpa_watchpoint': {
#                        'name': '[required] pipeline_name',
                        'environment': '[optional] environment, e.g., test or production',
                        'stage': '[optional] a string representing the stage of this part of the pipeline',
                        'version': '[optional] a string representing the version of implementation'
                    }
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
        d = self._d.get('configuration')
        return d.get(field, default_value)

    def get_configuration(self):
        return self._d.get('configuration')
    
    def get_local_cache_file(self):
        return self.get_configuration().get('session_history_cache', 'session_history_cache.db')


    def get_db_mongo(self):
        port = self._get_db('port', 27017)
        if type(port) == str:
            port = int(port)
        return (self._get_db('host'),
                port,
                self._get_db('dbname'),
                self._get_db('user'),
                os.environ.get('MONGO_PASSWORD', ''))

    def get_controller_config(self):
        return self._d.get('dataculpa_controller')

    def connect_controller(self, pipeline_name, timeshift=0):
        # FIXME: maybe handle $<var> substitution, like we do in the 
        # FIXME: Snowflake connector
        cc = self.get_controller_config()
        host = cc.get('host')
        port = cc.get('port')
        access_id = cc.get('api_key')
        if access_id is None:
            FatalError("Missing api_key from .yaml config")

        secret = os.environ.get('DC_API_SECRET')
        if secret is None:
            FatalError("Missing DC_API_SECRET from environment or .env file")

        v = DataCulpaValidator(pipeline_name,
                               protocol=DataCulpaValidator.HTTP,
                               dc_host=host,
                               dc_port=port,
                               api_access_id=access_id,
                               api_secret=secret,
                               queue_window=1000, 
                               timeshift=timeshift)
        return v

    def get_db_collection_config(self):
        return self._get_db('collections')

    def get_table_config_for_table_name(self, name):
        for entry in self.get_db_collection_config():
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
        (self._mongo_client, self._mongo_db) = self._get_mongo_connection()
        return

    def do_fetch_live_table_list(self):
        live_tables = []

        for coll in self._mongo_db.list_collection_names():
            live_tables.append(coll)

        return live_tables

    def _get_mongo_connection(self):
        # mongodb
        # https://api.mongodb.com/python/current/examples/authentication.html
        # Some unsupported-by-us mechanisms documented at the above url.
        (host, port, dbname, user, password) = self.get_db_mongo()

        client = MongoClient(host, port)
        db = client[dbname]
        return (client, db)


class SessionHistory:
    def __init__(self):
        self.history = {}
        self.config = None

    def set_config(self, config):
        assert isinstance(config, Config)
        self.config = config

    def add_history(self, table_name, field, value):
        assert self.config is not None
        self.history[table_name] = (field, value)
        return
    
    def has_history(self, table_name):
        return self.history.get(table_name) is not None

    def get_history(self, table_name):
        return self.history.get(table_name)
    
    def _get_existing_tables(self, cache_path):
        assert self.config is not None
        _tables = []
        c = sqlite3.connect(cache_path)
        r = c.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'")
        for row in r:
            _tables.append(row[0])
        return _tables

    def _handle_new_cache(self, cache_path):
        assert self.config is not None
        _tables = self._get_existing_tables(cache_path)

        c = sqlite3.connect(cache_path)
        if not ("cache" in _tables):
            c.execute("create table cache (object_name text unique, field_name text, field_value)")

        if not ("sql_log" in _tables):
            c.execute("create table sql_log (sql text, object_name text, Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")

        c.commit()

        # endif
    
        return

    def append_sql_log(self, table_name, sql_stmt):
        assert self.config is not None
        assert isinstance(table_name, str)
        assert isinstance(sql_stmt, str)

        cache_path = self.config.get_local_cache_file()
        c = sqlite3.connect(cache_path)
        self._handle_new_cache(cache_path)
        c.execute("insert into sql_log (sql, object_name) values (?,?)", (sql_stmt, table_name))
        c.commit()
        return

    def save(self):
        assert self.config is not None
        # write to disk
        cache_path = self.config.get_local_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        for table, f_pair in self.history.items():
            (fn, fv) = f_pair
            fv_pickle = pickle.dumps(fv)
            # Note that this might be dangerous if we add new fields later and we don't set them all...
            #print(table, fn, fv)
            c.execute("insert or replace into cache (object_name, field_name, field_value) values (?,?,?)", 
                      (table, fn, fv_pickle))

        c.commit()

        return
    
    def load(self):
        assert self.config is not None
        print("load...")
        time.sleep(1)

        # read from disk
        cache_path = self.config.get_local_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        r = c.execute("select object_name, field_name, field_value from cache")
        for row in r:
            (table, fn, fv_pickle) = row
            fv = pickle.loads(fv_pickle)
            self.add_history(table, fn, fv)
        # endfor
        return


gCache = SessionHistory()


def do_initdb(filename):
    print("Initialize new file")
    config = Config()
    config.save(filename)
    # Put out an .env template too.
    with open(filename + ".env", "w") as f:
        f.write("DC_API_SECRET=fill_me_in\n")
        f.write("#MONGO_PASSWORD=[optional]\n")
        f.close()

    return

def do_test_config(fname):
    if not os.path.exists(fname):
        print("%s does NOT exist!" % fname)
        os._exit(1)
        return

    # load the config
    config = Config()
    config.load(fname)

    (host, port, dbname, user, password) = config.get_db_mongo()
    client = MongoClient(host, port)


    # get what exists [that we can see anyway]
    d = DiscoverDatabasesAndCollections(client)
    
    db_names = list(d.keys())
    db_names.sort()

    if len(db_names) == 0:
        FatalError("No databases found on mongo server")
        return
    
    config_dict = config.get_configuration()
    if config_dict is None:
        FatalError("Missing 'configuration' section in yaml.")
        return
    
    dbname = config_dict.get('dbname')
    if dbname is None:
        FatalError("No 'dbname' attribute in 'configuration' section in yaml.")
        return
    
    if not dbname in db_names:
        FatalError("Configured dbname '%s' does not appear in discovered list of databases: %s" % (dbname, db_names))
        return

    # now test the collections...
    colls = d.get(dbname)
    #print(colls)

    # FIXME: can we read a record from each configured collection?
    config_colls = config.get_db_collection_config()
    #print(config_colls)
    for cc in config_colls:
        coll_name = cc.get('collection')
        if not coll_name in colls:
            print("Warning: a configured collection '%s' doesn't appear in the database; check this with --discover" % coll_name)
    return config


def DiscoverDatabasesAndCollections(client):
    d = {}

    for db in client.list_databases():
        # find collections in the database
        db_name = db.get('name', None)
        if db_name is None:
            continue
            continue

        the_db = client[db_name]
        for coll in the_db.list_collection_names():
            existing = d.get(db_name, [])
            existing.append(coll)
            d[db_name] = existing
    return d

def do_discover(fname, counts=False):
    # We also want to discover the databases for the given server, especially if none is specified.

    # We want to discover the collections in the given database.

    config = Config()
    config.load(fname)

    (host, port, dbname, user, password) = config.get_db_mongo()

    client = MongoClient(host, port)

    d = DiscoverDatabasesAndCollections(client)
    
    db_names = list(d.keys())
    db_names.sort()

    for db in db_names:
        print("%s:" % db)
        cList = d.get(db, [])
        cList.sort()
        for cl in cList:
            if counts:
                db_handle = client[db]
                coll_handle = db_handle[cl]
                total = coll_handle.count()

                # create synthetic id.
                fake_id = bson.ObjectId().from_datetime(datetime.utcnow() - timedelta(days=30))
                recent = coll_handle.find({"_id": { "$gt": fake_id }}).count()
                print("   %30s   total = %10s, recent = %10s" % (cl, total, recent))
            else:
                print("   %s" % cl)
        print("")
    # endfor

    return


def do_add(fname, in_db_name):
    #print("do_add")


    # load the config
    # generate a dict object.
    config = Config()
    config.load(fname)

    (host, port, dbname, user, password) = config.get_db_mongo()

    if in_db_name is None:
        if dbname is None:
            sys.stderr.write("No dbname specified in .yaml config and no --database passed.\n")
            os._exit(2)
        else:
            in_db_name = dbname

    client = MongoClient(host, port)
    d = DiscoverDatabasesAndCollections(client)

    cc_list = []

    #print("running on %s" % in_db_name)

    for db in list(d.keys()):
        if db != in_db_name:
            continue

        cList = d.get(db, [])
        cList.sort()
        for cl in cList:
            cc = { 'collection': cl,
                   'dataculpa_watchpoint': 'auto-' + db + "-" + cl,
                   'enabled': True,
                 }
            cc_list.append(cc)
        # endfor
    # endfor

    if len(cc_list) == 0:
        sys.stderr.write("Either no database named %s or it is empty." % in_db_name)
        os._exit(3)
    # endif

    db_config = { 'dbname': in_db_name,
                  'collections': cc_list
                }

    print("Example yaml to work with:")
    print("--------------------------")

    yaml.safe_dump(db_config, sys.stdout, default_flow_style=False)

    return


def FetchCollection(name, config, watchpoint, use_timeshift, timeshift_field):
    print("use_timeshift = %s; timeshift_field = %s", (use_timeshift, timeshift_field))
    db = config._mongo_db
    collection = db[name]

    gCache.load()

    marker_pair = gCache.get_history(name)
    
    query_constraints = {}

    if marker_pair is not None:
        (fk, fv) = marker_pair
        # do something with fk and fv.
        print("found marker_pair: %s, %s" % (fk, fv))
        query_constraints[fk] = { '$gt': fv }

    # I guess we need to sort by _id ASC
    if use_timeshift and timeshift_field is None:
        timeshift_field = "_id"

    if timeshift_field is not None:
        result = collection.find(query_constraints).sort(timeshift_field, ASCENDING)

    dc = None

    # OK, walk the results.
    last_id = None
    last_date = None
    record_count = 0
    last_had_broken_id = False

    for r in result:
        if record_count == 0:
            last_id = r.get('_id')
        record_count += 1

        if use_timeshift:
            this_id = r.get(timeshift_field)
            this_date = None

            if isinstance(this_id, datetime):
                # not isinstance(this_id, bson.ObjectId): 
                #print("can't infer time from non-ObjectId _id: ", this_id)
                #if not last_had_broken_id:
                #    last_date = None
                #    if dc is not None:
                #        dc.queue_commit()
                #        dc = None
                #    last_had_broken_id = True
                # look at the timezone...
                this_date = this_id.date()
            elif isinstance(this_id, str):
                # we're going to struggle here...
                print("Timeshift field \"%s\" came back as a string on record id \"%s\" which is going to be very hard to run date compares... clearly we need to handle this better than exiting."
                        % (timeshift_field, r.get('_id')))
                os._exit(2)
                dt = DateUtilParse(this_id)
            elif isinstance(this_id, bson.ObjectId):        
                # extract the day
                this_date = this_id.generation_time.date()
            # endif

            if last_date is not None and this_date != last_date:
                # if the day has moved, close the queue and open it again.
                (_queue_id, _result) = dc.queue_commit()
                print("server_result: __%s__" % _result)
                dc = None
            # endif

            if dc is None: # this gets run our first time through too.
                this_date_dt = datetime(year=this_date.year, month=this_date.month, day=this_date.day)
                dt = (datetime.utcnow() - this_date_dt).total_seconds()
                dt = int(dt) # old dataculpa client library expects an int.
                print("dt = ", dt)
                dc = config.connect_controller(watchpoint, timeshift=dt)

            # endif

            last_date = this_date
        # endif

        if dc is None: # note we can enter here even if use_timeshift is true if we hit a weird _id field
            dc = config.connect_controller(watchpoint)

        dc.queue_record(r, jsonEncoder=MongoJSONEncoder)

    # endfor

    if last_id is not None:
        gCache.add_history(name, "_id", last_id)
        gCache.save()
    # endif

    print("new records found = ", record_count)

    # FIXME: add metadata about the query...
    # dc.queue_metadata(meta)
    if dc is not None:
        (_queue_id, _result) = dc.queue_commit()
        print("server_result: __%s__" % _result)

    # FIXME: On error, rollback the cache

    return

def do_run(fname):
    print("do_run")

    # load the config
    config = Config()
    config.load(fname)
    gCache.set_config(config)

    # connect to the db.
    # if we can't connect, log an error to the cache. -- and post metadata.
    config.do_connect() # FIXME: handle errors.

    collection_config = config.get_db_collection_config()

    print("collection_config = ", collection_config)

    for cc in collection_config:
        name            = cc.get('collection', None)
        watchpoint      = cc.get('dataculpa_watchpoint', None)
        enabled         = cc.get('enabled', True)
        use_timeshift   = cc.get('use_timeshift', True)
        timeshift_field = cc.get('timeshift_field', None)

        if not enabled:
            # log it, etc.
            # probably bring over the tracing logging from other modules.
            print("collection %s enabled set to False; skipping" % name)
            continue

        # see if we have a history
        FetchCollection(name, config, watchpoint, use_timeshift, timeshift_field)

    return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env",
                    help="Use provided env file instead of default .env")

    ap.add_argument("--init",     help="Init a yaml config file to fill in.")
    ap.add_argument("--discover", help="Run the specified configuration to discover available databases/collections. The yaml must have the host and port (if not running on the default port) specified.")
    ap.add_argument("--add",      help="Requires --database flag if database is not populated in the passed YAML file. Generate YAML config (to stdout) for un-configured databases and collections that can be inserted into the YAML for the specified configuration.")
    ap.add_argument("--test",     help="Test the configuration specified.")
    ap.add_argument("--run",      help="Normal operation: run the pipeline")

    # FIXME: implement add subcommand.
#    ap_add = subparsers.add_parser("--add")
    ap.add_argument("--database", help="For --add: Operate on the specified database name")
    ap.add_argument("--counts",   help="For --discover: show table counts total and last 30 days of records", action='store_true')

    args = ap.parse_args()

    if args.init:
        do_initdb(args.init)
        return
    else:
        env_path = ".env"
        if args.env:
            env_path = args.env
        if not os.path.exists(env_path):
            sys.stderr.write("Error: missing env file at %s\n" % os.path.realpath(env_path))
            os._exit(1)
            return
        # endif

        if args.discover:
            do_counts = False
            if args.counts:
                do_counts = True
            dotenv.load_dotenv(env_path)
            do_discover(args.discover, counts=do_counts)
            return
        elif args.test:
            dotenv.load_dotenv(env_path)
            do_test_config(args.test)
            return
        elif args.add:
            dotenv.load_dotenv(env_path)
            do_add(args.add, args.database)
            return
        elif args.run:
            dotenv.load_dotenv(env_path)
            do_run(args.run)
            return
        # endif
    # endif

    ap.print_help()
    return


if __name__ == "__main__":
    main()
