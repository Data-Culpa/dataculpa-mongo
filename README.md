# dataculpa-mongo
MongoDB connector for Data Culpa for monitoring ongoing quality metrics in Mongo databases.

This connector conforms to the [Data Culpa connector template](https://github.com/Data-Culpa/connector-template).


## Getting Started

0. Identify the host you want to run the pipeline on; this can be run on the same host as a Data Culpa Validator controller, or it can be run on some other host, such as a replica MongoDB host. The host will need to be able to access the MongoDB and the Data Culpa Validator controller.
1. Clone the repo (or just mongodatalake.py)
2. Install python dependencies (python3):
```
pip install python-dotenv pymongo dataculpa-client
```
3. Run `python3 mongo-dataculpa.py --init mongodb.yaml` (using a .yaml filename of your choice) to set up stub files to populate. This creates a `mongodb.yaml` and a `mongodb.yaml.env`. You can pass in the .env file name to future executions of `mongo-dataculpa.py` with the `-e` flag or run `ln -s mongodb.yaml.env .env` (or just move the file to `.env`).

4. Modify the `.env` file with the following keys (if you're running Mongo without a password, no need to specify the MONGO_PASSWORD key):

```
# API key to access the storage.
DC_CONTROLLER_SECRET = secret-here   # Create a new API secret in the Data Culpa Validator UI
MONGO_PASSWORD = secret-here 
```

4. You can now query your Mongo host to see what your configuration can see for connecting. Run `python3 mongo-dataculpa.py --discover mongodb.yaml` to see a list of databases and collections; note that `--discover` doesn't mutate any configuration files or other state:

```
admin:
   system.version

config:
   system.sessions

local:
   startup_log

mydb:
   my_analytics
   my_facility
   my_payroll
```

5. Run ```python3 mongo-dataculpa.py --add mongodb.yaml --database mydb``` (using the `mydb` value of interest from `--discover`) to print out example YAML that you can place into the `mongodb.yaml` file under the `collections` section:

```
$ python3 mongo-dataculpa.py --add mongodb.yaml --database masdb
Example yaml to work with:
--------------------------
collections:
- collection: my_analytics
  dataculpa_watchpoint: auto-mydb-my_analytics
  enabled: true
- collection: my_facility
  dataculpa_watchpoint: auto-mydb-my_facility
  enabled: true
...
```
The `dataculpa_watchpoint` string is what the name of the collection will be in Data Culpa; you can change this to anything you want. If you edit it later, you'll start a new namespace in Data Culpa. You can omit the `enabled` flag (default is true if it is missing); it is provided as a convenience to turn off watching a collection if needed.

Anyway, grab the parts you want and place them in the .yaml.

6. You can test your .yaml file with the `--test` flag. This will also check that at least one record can be pulled from each collection that is specified.

7. Once the `--test` is working, you can start pulling data for real with `--run`. You can kick off `--run` from cron or whatever orchestration system you want to use.

```

```

## Notes

1. The design of the connector and configuration is that only one database can be used per connector config file. Of course you can create multiple configuration files with their own names and pull in multiple databases through a single connector, but it will need to be called from your orchestration system multiple times and with the appropriate arguments (config files).   

## Invocation

The ```mongo-datalake.py``` script is intended to be invoked from cron or other orchestration systems. You can run it as frequently as you wish; you can spread out instances to isolate collections or different databases with different yaml configuration files. We recommend pointing the script at a replica of your database to reduce impact on the life system.

## Future Improvements

There are many improvements we are considering for this module, including distribution as a Docker container. You can get in touch by writing to hello@dataculpa.com or opening issues in this repository.

## SaaS deployment

Our hosted SaaS includes Mongo and other connectors and a GUI for configuration. If you'd like to try it out before general availability, drop a line to hello@dataculpa.com.
