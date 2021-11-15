import pyArango.theExceptions
from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
from arango_schema import *
from typing import *


def init_database(conn: Connection, name: str) -> Database:
    if conn.hasDatabase(name=name) is False:
        conn.createDatabase(name=name)
    return conn[name]


def init_collection(database: Database, name: str, class_name: str, geo_index: bool):
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name)
    if geo_index:
        ensureGeoJsonIndex(database[name], fields=['geo_location'], name='geo_location', geoJson=True)
    return database[name]


def init_edges(database: Database, name: str, class_name: str) -> Edges:
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name)
    return database[name]


def init_graph(database: Database, class_name: str):
    if database.hasGraph(class_name) is False:
        database.createGraph(class_name)
    return database.graphs[class_name]


def update_daily_balances(database: Database, balances_data: List[dict]):
    for doc in balances_data:
        aql = f"""upsert {{_key: '{doc['_key']}'}}
        insert {doc}
        update {{ daily_balances: append(OLD.daily_balances, {doc['daily_balances']}) }} in balances"""
        database.AQLQuery(aql)


def remove_witnesses_before_time(database: Database, cutoff_time: int):
    aql = f"""for witness in witnesses
    filter witness.time < {cutoff_time}
    remove witness in witnesses OPTIONS {{ waitForSync: true }}"""
    database.AQLQuery(aql)


def update_rewards(database: Database, rewards_data: List[dict]):
    for doc in rewards_data:
        aql = f"""for hotspot in hotspots
        update {{_key: '{doc['address']}', rewards_5d: {doc['rewards']}}} in hotspots"""
        try:
            database.AQLQuery(aql)
        except pyArango.theExceptions.AQLQueryError:
            # this catches '1Wh4bh' gateway
            continue