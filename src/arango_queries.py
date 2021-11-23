import multiprocessing

import pyArango.theExceptions
from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
from arango_schema import *
from typing import *
import networkx as nx
from blockchain_queries import *
from multiprocessing import Process, cpu_count, Manager
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os
from math import isnan
from statistics import mean


logging.basicConfig(filename='../logs/etl.log', encoding='utf-8', level=logging.INFO)


def init_database(conn: Connection, name: str) -> Database:
    """
    Creates arango database if it doesn't already exist.
    :param conn: The PyArango connection.
    :param name: The database name.
    :return: The PyArango Database object.
    """
    if conn.hasDatabase(name=name) is False:
        conn.createDatabase(name=name)
    return conn[name]


def init_collection(database: Database, name: str, class_name: str, geo_index: bool):
    """
    Creates arango collection if it doesn't already exist.
    :param database: The PyArango Database object.
    :param name: The collection name.
    :param class_name: The collection class name (see arango_schema.py).
    :param geo_index: bool if the collection should include a geo index (e.g. hotspot coords).
    :return: The PyArango Collection object.
    """
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name, waitForSync=True)
    if geo_index:
        ensureGeoJsonIndex(database[name], fields=['geo_location'], name='geo_location', geoJson=True)
    return database[name]


def init_edges(database: Database, name: str, class_name: str) -> Edges:
    """
    Creates arango edge collection if it doesn't already exist.
    :param database: The PyArango Database object.
    :param name: The collection name.
    :param class_name: The collection class name (see arango_schema.py).
    :return: The PyArango Edges object.
    """
    if database.hasCollection(name) is False:
        database.createCollection(className=class_name, name=name, waitForSync=True)
    return database[name]


def init_graph(database: Database, class_name: str) -> Graph:
    """
    Creates an arango graph if it doesn't already exist.
    :param database: The PyArango Database object.
    :param class_name: The graph class name (see arango_schema.py)
    :return: The PyArango Graph object.
    """
    if database.hasGraph(class_name) is False:
        database.createGraph(class_name)
    return database.graphs[class_name]


def update_daily_balances(database: Database, balances_data: List[dict]):
    """
    Deprecated in favor of more optimized methods.
    :param database:
    :param balances_data:
    """
    for doc in balances_data:
        aql = f"""upsert {{_key: '{doc['_key']}'}}
        insert {doc}
        update {{ daily_balances: append(OLD.daily_balances, {doc['daily_balances']}) }} in balances"""
        database.AQLQuery(aql)


def remove_witnesses_before_time(database: Database, cutoff_time: int):
    """
    Remove witness edges before a certain timestamp.
    :param database: The PyArango Database object.
    :param cutoff_time: The cutoff timestamp.
    """
    aql = f"""for witness in witnesses
    filter witness.time < {cutoff_time}
    remove witness in witnesses OPTIONS {{ waitForSync: true }}"""
    database.AQLQuery(aql)


def update_rewards(database: Database, rewards_data: List[dict]):
    """
    Deprecated in favor of more optimized options.
    :param database:
    :param rewards_data:
    """
    for doc in rewards_data:
        aql = f"""for hotspot in hotspots
        update {{_key: '{doc['address']}', rewards_5d: {doc['rewards']}}} in hotspots"""
        try:
            database.AQLQuery(aql)
        except pyArango.theExceptions.AQLQueryError:
            # this catches '1Wh4bh' gateway
            continue


def get_cities_list(database: Database) -> List[str]:
    """
    Returns the unique city keys in the cities collection.
    :param database: The PyArango Database object.
    :return: The list of city_key strings (md5 hash of city_id in locations table).
    """
    aql = """for city in cities
    return {city_key: city._key}"""
    return [city['city_key'] for city in database.fetch_list(aql)]


def city_witness_graph_metrics_bulk(return_dict: dict, city_list: List[str], min_city_size: int):
    """
    Multiprocessing target for extracting city graph metrics for each city in city_list.
    :param return_dict: The multiprocessing Manager()'s dict destination for outputs.
    :param city_list: The list of city keys in the cities collection.
    :param min_city_size: Only consider cities with more than this many hotspots.
    """
    nan_to_num = lambda x: 0 if isnan(x) else x
    # if running in parallel, need independent connections
    connection = Connection(
        arangoURL=os.getenv('ARANGO_URL'),
        username=os.getenv('ARANGO_USERNAME'),
        password=os.getenv('ARANGO_PASSWORD')
    )
    database = connection['helium']
    hotspots = database['hotspots']
    for city in city_list:
        # only consider valid witness paths
        aql = f"""
        for hotspot in hotspots
        filter hotspot.location_details.city_key == '{city}'
        for v, e, p in 1..1 outbound hotspot witnesses
            filter e.is_valid
            let distance_m = GEO_DISTANCE(p.vertices[0].geo_location, p.vertices[1].geo_location)
            RETURN {{_from: last(split(e._from, '/')), _to: last(split(e._to, '/')), distance_m: distance_m}}
        """
        try:
            result = database.fetch_list(aql)
        except pyArango.theExceptions.AQLFetchError:
            continue
        if len(result) < min_city_size:
            continue
        g = nx.DiGraph()
        edges = [(edge.values()) for edge in result]
        g.add_weighted_edges_from(edges)
        bc = nx.betweenness_centrality(g)
        bc_mean = mean(bc.values())
        pg = nx.pagerank(g)
        pg_mean = mean(pg.values())
        # (hubs, authorities) = nx.algorithms.hits(g) # not sure how useful this is
        features = [{
            '_key': key,
            'betweenness_centrality': nan_to_num(bc[key]),
            'betweenness_centrality_n': nan_to_num(bc[key] / bc_mean),
            'pagerank': nan_to_num(pg[key]),
            'pagerank_n': nan_to_num(pg[key] / pg_mean)}
            for key in pg.keys()]
        try:
            hotspots.importBulk(features, onDuplicate='update')
            return_dict[city] = len(features)
        except pyArango.theExceptions.CreationError:
            logging.info(f'Arango did not like this JSON: {features}')
            continue


def parallel_city_graph_processing(database: Database, min_city_size: int) -> Tuple[int, int]:
    """
    Parallel method for allocating processes to city graph analysis.
    :param database:
    :param min_city_size:
    :return:
    """
    manager = Manager()
    return_dict = manager.dict()
    cities_list = get_cities_list(database)
    logging.info(f'Generating graphs/metrics for {len(cities_list)} unique cities...')
    city_chunks = []
    chunk_size = int(len(cities_list) / cpu_count())
    for i in range(cpu_count()):
        chunk_end = min([i + chunk_size, len(cities_list)])
        city_chunks.append(cities_list[i:chunk_end])
    processes = []
    # naive domain decomposition...split up the cities list into equal segments
    for i in range(cpu_count()):
        p = Process(target=city_witness_graph_metrics_bulk, args=(return_dict, city_chunks[i], min_city_size))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    return len(return_dict.keys()), sum(return_dict.values())


def import_batched(batched_query: BatchedQuery, collection: Collection, on_duplicate: str = 'update') -> int:
    """
    Import data to arango in batches.
    :param batched_query: The BatchedQuery object (see blockchain_queries.py)
    :param collection:
    :param on_duplicate:
    :return:
    """
    num_docs_imported = 0
    while True:
        batch = batched_query.get_next_batch()
        if len(batch) > 0:
            response = collection.importBulk(batch, onDuplicate=on_duplicate, waitForSync=True)
            logging.info(f'Batch import response: {response}')
            num_docs_imported += response['updated'] + response['created']
        else:
            break
    return num_docs_imported


def import_batched_mp(return_dict, proc_num: int, batched_query: BatchedQuery, collection_name: str, on_duplicate: str = 'update'):
    """
    Parallel target for importing data from batched queries.
    :param return_dict:
    :param proc_num:
    :param batched_query:
    :param collection_name:
    :param on_duplicate:
    :return:
    """
    # if running in parallel, need independent connections
    connection = Connection(
        arangoURL=os.getenv('ARANGO_URL'),
        username=os.getenv('ARANGO_USERNAME'),
        password=os.getenv('ARANGO_PASSWORD')
    )
    database = connection['helium']
    collection = database[collection_name]
    num_docs_imported = 0
    while True:
        batch = batched_query.get_next_batch()
        if len(batch) > 0:
            response = collection.importBulk(batch, onDuplicate=on_duplicate, waitForSync=True)
            logging.info(f'Batch import response: {response}')
            num_docs_imported += response['updated'] + response['created']
        else:
            break
    return_dict[proc_num] = num_docs_imported


def update_batched(batched_query: BatchedQuery, database: Database) -> int:
    """
    Deprecated. Faster to just import with onDuplicate='update' flag.
    :param batched_query:
    :param database:
    :return:
    """
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
    """
    Deprecated. Faster to just import with onDuplicate='update' flag.
    :param batched_query:
    :param database:
    :return:
    """
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


def import_rewards_batched(session: Session, batch_size: int, hotspots: Collection, min_time: int, max_time: int) -> int:
    batched_query = GatewayRewardsBatchedQuery(session, batch_size, min_time, max_time)
    return import_batched(batched_query, hotspots, on_duplicate='update')


def import_payments_batched(session: Session, batch_size: int, payments: Collection, min_time: int, max_time: int) -> int:
    batched_query = RecentPaymentsBatchedQuery(session, batch_size, min_time, max_time)
    return import_batched(batched_query, payments, on_duplicate='ignore')


def import_cities_batched(session: Session, batch_size: int, cities: Collection) -> int:
    batched_query = CitiesBatchedQuery(session, batch_size)
    return import_batched(batched_query, cities, on_duplicate='ignore')


def parallel_import_time_chunks(sessionmaker: sessionmaker, batch_size: int, collection_name: str, min_time: int, max_time: int, on_duplicate: str = 'ignore') -> int:
    manager = Manager()
    return_dict = manager.dict()
    processes, sessions = [], []
    # naive domain decomposition...split up the time into equal segments
    p_min_time, p_max_time = min_time, min_time + int((max_time - min_time) / cpu_count())
    for i in range(cpu_count()):
        session = sessionmaker()
        if collection_name == 'payments':
            batched_query = RecentPaymentsBatchedQuery(session, batch_size, p_min_time, p_max_time)
        elif collection_name == 'witnesses':
            batched_query = RecentWitnessesBatchedQuery(session, batch_size, p_min_time, p_max_time)
        elif collection_name == 'balances':
            batched_query = DailyBalancesBatchedQuery(session.bind, batch_size, p_min_time, p_max_time)
        else:
            raise ValueError(f'Unexpected collection_name: {collection_name}')
        # ignore duplicates - going to assume that things will not change much over 5 days
        p = Process(target=import_batched_mp, args=(return_dict, i, batched_query, collection_name, on_duplicate,))
        processes.append(p)
        sessions.append(session)
        p_min_time = p_max_time
        p_max_time += int((max_time - min_time) / cpu_count())
        p.start()
    for p in processes:
        p.join()
    for s in sessions:
        s.close()
    return sum(return_dict.values())


def import_witnesses_batched(session: Session, batch_size: int, witnesses: Collection, min_time: int, max_time: int) -> int:
    batched_query = RecentWitnessesBatchedQuery(session, batch_size, min_time, max_time)
    return import_batched(batched_query, witnesses, on_duplicate='ignore')


def import_witnesses_mp(sessionmaker: sessionmaker, batch_size: int, min_time: int, max_time: int) -> int:
    return parallel_import_time_chunks(sessionmaker, batch_size, 'witnesses', min_time, max_time)


def import_payments_mp(sessionmaker: sessionmaker, batch_size: int, min_time: int, max_time: int) -> int:
    return parallel_import_time_chunks(sessionmaker, batch_size, 'payments', min_time, max_time)


def import_daily_balances_batched(sessionmaker: sessionmaker, batch_size: int, min_time: int, max_time: int) -> int:
    return parallel_import_time_chunks(sessionmaker, batch_size, 'balances', min_time, max_time)
