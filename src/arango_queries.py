import pyArango.theExceptions
from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
from arango_schema import *
from typing import *
import networkx as nx
from blockchain_queries import *


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


def global_witness_graph_metrics(database: Database):
    aql = """for hotspot in hotspots
    for v, e, p in 1..1 outbound hotspot witnesses
        let distance_m = GEO_DISTANCE(p.vertices[0].geo_location, p.vertices[1].geo_location)
        RETURN {_from: last(split(e._from, '/')), _to: last(split(e._to, '/')), distance_m: distance_m}
    """
    g = nx.DiGraph()
    for edge_batch in database.fetch_list_as_batches(aql, batch_size=1000):
        edges = [(edge.values()) for edge in edge_batch]
        g.add_weighted_edges_from(edges)

    bc = nx.betweenness_centrality(g)
    pg = nx.pagerank(g)
    (hubs, authorities) = nx.algorithms.hits(g)
    features = {key: {'betweenness_centrality': bc[key],
                      'pagerank': pg[key],
                      'hub_score': hubs[key],
                      'authority_score': authorities[key]}
                for key in hubs.keys()}

    for address in features.keys():
        aql = f"""for hotspot in hotspots
        update {{_key: '{address}', 
        betweenness_centrality: {features[address]['betweenness_centrality']},
        pagerank: {features[address]['pagerank']},
        hub_score: {features[address]['hub_score']},
        authority_score: {features[address]['authority_score']}
        }} in hotspots"""
        database.AQLQuery(aql)


def import_batched(batched_query: BatchedQuery, collection: Collection, on_duplicate: str = 'update') -> int:
    num_docs_imported = 0
    while True:
        batch = batched_query.get_next_batch()
        if len(batch) > 0:
            collection.importBulk(batch, onDuplicate=on_duplicate)
            num_docs_imported += len(batch)
        else:
            break
    return num_docs_imported


def update_batched(batched_query: BatchedQuery, database: Database) -> int:
    num_docs_imported = 0
    while True:
        batch = batched_query.get_next_batch()
        if len(batch) > 0:
            update_rewards(database, batch)
            num_docs_imported += len(batch)
        else:
            break
    return num_docs_imported


def update_balances_batched(batched_query: BatchedQuery, database: Database) -> int:
    num_docs_imported = 0
    while True:
        batch = batched_query.get_next_batch()
        if len(batch) > 0:
            update_daily_balances(database, batch)
            num_docs_imported += len(batch)
        else:
            break
    return num_docs_imported


def import_accounts_batched(session: Session, batch_size: int, accounts: Collection) -> int:
    batched_query = AccountInventoryBatchedQuery(session, batch_size=batch_size)
    return import_batched(batched_query, accounts, on_duplicate='update')


def import_hotspots_batched(session: Session, batch_size: int, hotspots: Collection) -> int:
    batched_query = GatewayInventoryBatchedQuery(session, batch_size=batch_size)
    return import_batched(batched_query, hotspots, on_duplicate='update')


def import_rewards_batched(session: Session, batch_size: int, database: Database, min_time: int, max_time: int) -> int:
    batched_query = GatewayRewardsBatchedQuery(session, batch_size, min_time, max_time)
    return update_batched(batched_query, database)


def import_payments_batched(session: Session, batch_size: int, payments: Collection, min_time: int, max_time: int) -> int:
    batched_query = RecentPaymentsBatchedQuery(session, batch_size, min_time, max_time)
    return import_batched(batched_query, payments, on_duplicate='ignore')


def import_witnesses_batched(session: Session, batch_size: int, witnesses: Collection, min_time: int, max_time: int) -> int:
    batched_query = RecentWitnessesBatchedQuery(session, batch_size, min_time, max_time)
    return import_batched(batched_query, witnesses, on_duplicate='replace')


def import_daily_balances_batched(engine: Engine, batch_size: int, database: Database, min_time: int, max_time: int) -> int:
    batched_query = DailyBalancesBatchedQuery(engine, batch_size, min_time, max_time)
    return update_balances_batched(batched_query, database)