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


def init_collection(database: Database, name: str, class_name: str, geo_index: bool) -> Collection:
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name)
    if geo_index:
        database[name].ensureGeoJsonIndex(fields=['geo_location'], name='geo_location', geoJson=True)
    return database[name]


def init_edges(database: Database, name: str, class_name: str) -> Edges:
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name)
    return database[name]


def init_graph(database: Database, class_name: str):
    if database.hasGraph(class_name) is False:
        database.createGraph(class_name)
    return database.graphs[class_name]


def process_query(database: Database, aql: str, raw_results: bool = True, batch_size: int = 100) -> List[dict]:
    result = database.AQLQuery(aql, rawResults=raw_results, batchSize=batch_size)
    result_set = result.response['result']
    while result.response['hasMore']:
        result_set.append(database.AQLQuery(aql, rawResults=True).response['result'])
    return result_set


def get_top_payment_totals(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from, to, payment_total}}"""
    totals = process_query(database, aql)
    return totals


def get_top_payment_counts(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_count = LENGTH(payment_groups)
    sort payment_count desc
    limit {n}
    return {{from, to, payment_count}}"""
    counts = process_query(database, aql)
    return counts


def get_top_flows_from_accounts(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from, payment_total}}"""
    totals = process_query(database, aql)
    return totals


def get_top_flows_to_accounts(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{to, payment_total}}"""
    totals = process_query(database, aql)
    return totals