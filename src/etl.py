import argparse

from blockchain_queries import *
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as PostgresSession
import os
from arango_queries import *
from pyArango.connection import *
import time
import logging


load_dotenv('../.env')
logging.basicConfig(filename='../logs/etl.log', encoding='utf-8', level=logging.INFO)


class HeliumArangoETL(object):
    """The HeliumArangoETL class pulls data from the relational blockchain database and transforms it into a native graph format before importing to ArangoDB.

    The focus is on functionality that is not already (easily) accessible via the Helium Blockchain API, like producing witness graphs or analyses of
    token flow. This ETL performs an initial sync of the blockchain before running a follower that ingests new blocks in chunks. Requires read access
    to a Postgres node (see https://github.com/helium/blockchain-etl) and read/write access to an Arango instance. Credentials are read from the .env
    file.

    Example usage:
    etl = HeliumArangoETL() # initializes the connections
    elt.start()             # starts the sync & follower

    """
    def __init__(self):
        """Initialize the ETL with connections and configure the required Arango database & collections."""

        self.min_block_diff_for_update = int(os.getenv('ETL_MIN_BLOCK_DIFF_FOR_UPDATE'))
        self.recent_witness_days_cutoff = int(os.getenv('ETL_RECENT_WITNESS_DAYS_CUTOFF'))
        self.batch_size = int(os.getenv('ETL_IMPORT_BATCH_SIZE'))

        arango_connection = Connection(
            arangoURL=os.getenv('ARANGO_URL'),
            username=os.getenv('ARANGO_USERNAME'),
            password=os.getenv('ARANGO_PASSWORD')
        )
        self.db = init_database(arango_connection, 'helium')

        self.postgres_engine = create_engine(os.getenv('POSTGRES_URL'))
        make_postgres_session = sessionmaker(bind=self.postgres_engine)
        self.postgres_session = make_postgres_session()

        self.hotspots = init_collection(self.db, name='hotspots', class_name='HotspotCollection', geo_index=True)
        self.accounts = init_collection(self.db, name='accounts', class_name='AccountCollection', geo_index=False)
        self.payments = init_edges(self.db, name='payments', class_name='PaymentEdges')
        self.balances = init_collection(self.db, name='balances', class_name='BalancesCollection', geo_index=False)
        self.witnesses = init_edges(self.db, name='witnesses', class_name='WitnessEdges')

        self.sync_height = self.initial_sync_chunk_size = int(os.getenv('ETL_INITIAL_SYNC_CHUNK_SIZE'))

        self.current_height = get_current_height(self.postgres_session)
        self.current_time = get_timestamp_by_block(self.postgres_session, self.current_height)

    def start(self):
        """Start the ETL daemon."""

        logging.info(f'\n\n===== PERFORMING INITIAL SYNC UP TO BLOCK {self.current_height} ====\n\n')
        min_time, max_time = 0, get_timestamp_by_block(self.postgres_session, self.initial_sync_chunk_size)

        self.sync_inventories()
        self.sync_dynamic_collections(min_time, max_time)
        self.follow()

    def sync_chunk(self, min_time: int, max_time: int):
        import_payments_batched(self.postgres_session, self.batch_size, self.payments, min_time, max_time)

        import_daily_balances_batched(self.postgres_engine, self.batch_size, self.db, min_time, max_time)

    def sync_inventories(self):
        """Inventories include collections/edges that we only want the most recent snapshot of, like hotspots, accounts, and, witness lists."""

        num_accounts_imported = import_accounts_batched(self.postgres_session, self.batch_size, self.accounts)
        logging.info(f'{num_accounts_imported} accounts imported from inventory. Beginning import of hotspots...')

        num_hotspots_imported = import_hotspots_batched(self.postgres_session, self.batch_size, self.hotspots)
        logging.info(f'{num_hotspots_imported} hotspots imported from inventory. Beginning import of witness lists...')

        min_witness_time = self.current_time - 3600*24*self.recent_witness_days_cutoff
        num_witnesses_imported = import_witnesses_batched(self.postgres_session, self.batch_size, self.witnesses, min_witness_time, self.current_time)
        # after importing new witnesses, remove old ones (this may be an interesting diff operation later on?)
        remove_witnesses_before_time(self.db, min_witness_time)
        logging.info(f'{num_witnesses_imported} witness paths reported over last {self.recent_witness_days_cutoff} days. Beginning import of rewards data...')

        # get rewards over same range as witnesses
        num_rewards_updated = import_rewards_batched(self.postgres_session, self.batch_size, self.db, min_witness_time, self.current_time)
        logging.info(f'Rewards data imported for {num_rewards_updated} hotspots. Beginning extraction of global graph metrics...')

        # run global graph analyses and update hotspots where applicable
        # TODO: graph analysis in batches
        global_witness_graph_metrics(self.db)
        logging.info(f'Global graph metrics applied. Beginning import of payments and balances...')

    def sync_dynamic_collections(self, min_time, max_time):
        """Dynamic collections include values/edges that we want to track over time, like payments and changes in balances."""

        while True:
            self.sync_chunk(min_time, max_time)

            logging.info(f'..payments and balances synced to block {self.sync_height} / {self.current_height}')
            self.sync_height += self.initial_sync_chunk_size
            min_time = max_time

            if self.sync_height > self.current_height:
                max_time = get_timestamp_by_block(self.postgres_session, self.current_height)
                self.sync_chunk(min_time, max_time)
                self.sync_height = self.current_height
                break
            else:
                max_time = get_timestamp_by_block(self.postgres_session, self.sync_height)

    def follow(self):
        """After initial sync, run this continuously. Change ETL_UPDATE_INTERVAL environment variable to check for updates more or less often."""

        update_interval_seconds = int(os.getenv('ETL_UPDATE_INTERVAL_SEC'))
        logging.info(f'Beginning periodic sync of token flow every {update_interval_seconds} seconds, according to TOKEN_FLOW_UPDATE_INTERVAL_SEC environment variable.')
        while True:
            time.sleep(update_interval_seconds)
            n_discovered_blocks = get_current_height(self.postgres_session) - self.current_height

            if n_discovered_blocks > self.min_block_diff_for_update:
                logging.info(f'{n_discovered_blocks} new blocks discovered. Re-syncing database.')
                min_time = get_timestamp_by_block(self.postgres_session, self.sync_height)
                max_time = get_timestamp_by_block(self.postgres_session, self.current_height)

                self.sync_inventories()
                self.sync_dynamic_collections(min_time, max_time)
            else:
                logging.info(f'Only {n_discovered_blocks} new blocks discovered. No re-sync this epoch.')



etl = HeliumArangoETL()
etl.start()


