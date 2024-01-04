from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

def get_registers_collection(db_name="anteia_prod"):
    db_user = os.environ.get('db_user')
    db_password = os.environ.get('db_password')

    client = MongoClient(
        f"mongodb+srv://{db_user}:{db_password}@data-cluster.1h7wi.mongodb.net/?retryWrites=true&w=majority"
    )

    coreid = client[db_name]
    return coreid["Registration"]

def get_clientinfo_collection(db_name="anteia_prod"):
    db_user = os.environ.get('db_user')
    db_password = os.environ.get('db_password')

    client = MongoClient(
        f"mongodb+srv://{db_user}:{db_password}@data-cluster.1h7wi.mongodb.net/?retryWrites=true&w=majority"
    )

    coreid = client[db_name]
    return coreid["clients"]

def get_registershflow_collection(db_name="hyperflow_prod"):
    db_user = os.environ.get('db_user')
    db_password = os.environ.get('db_password')

    client = MongoClient(
        f"mongodb+srv://{db_user}:{db_password}@data-cluster.1h7wi.mongodb.net/?retryWrites=true&w=majority"
    )

    hyperflow = client[db_name]
    return hyperflow["ExecutedFlow"]

def get_registerssflow_collection(db_name="smartflow-prod"):
    db_user = os.environ.get('db_user')
    db_password = os.environ.get('db_password')

    client = MongoClient(
        f"mongodb+srv://{db_user}:{db_password}@data-cluster.1h7wi.mongodb.net/?retryWrites=true&w=majority"
    )

    smartflow = client[db_name]
    return smartflow["FlowProcess"]
