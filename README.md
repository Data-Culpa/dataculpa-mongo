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
3. Create a .env file with the following keys (if you're running Mongo without a password, no need to specify the DB_PASSWORD key):

```
# API key to access the storage.
DC_CONTROLLER_SECRET = secret-here   # Create a new API secret in the Data Culpa Validator UI
DB_PASSWORD = secret-here 
```

4. Run ```mongo-dataculpa.py --init your.yaml``` to generate a template yaml to fill in connection coordinates. Note that we always keep secrets in the .env and not the yaml, so that the yaml file will be safe to check into source control or otherwise distribute in your organization, etc.

5. Once you have your yaml file edited, run ```mongo-dataculpa.py --test your.yaml``` to test the connections to the database and the Data Culpa Validator controller.

6. Run ```mongo-dataculpa.py --sync-config your.yaml``` to add metadata to your yaml file for each collection found in your MongoDB and example parameters of how to treat each collection and new collections:

```
FIXME: add example yaml
```

## Invocation

The ```mongodatalake.py``` script is intended to be invoked from cron or other orchestration systems. You can run it as frequently as you wish; you can spread out instances to isolate collections or different databases with different yaml configuration files. We recommend pointing the script at a replica of your database to reduce impact on the life system.

## Future Improvements

There are many improvements we are considering for this module. You can get in touch by writing to hello@dataculpa.com or opening issues in this repository.

## SaaS deployment

Our hosted SaaS includes Mongo and other connectors and a GUI for configuration. If you'd like to try it out before general availability, drop a line to hello@dataculpa.com.
