from blockchain_queries import *
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import os
from arango_queries import *
from pyArango.connection import *

# check if arango exists and is properly initialized, extract data from db, package into arango-friendly format, bulk import
# run in loop
# make a collection for latest updates? or just include as a field

load_dotenv()

#### ARANGO CONFIG ####
arango_connection = Connection(
    arangoURL=os.getenv('ARANGO_URL'),
    username=os.getenv('ARANGO_USERNAME'),
    password=os.getenv('ARANGO_PASSWORD')
)
#### POSTGRES CONFIG ####
postgres_engine = create_engine(os.getenv('POSTGRES_URL'))
Session = sessionmaker(bind=postgres_engine)
session = Session()


db = init_database(arango_connection, 'helium')

accounts = init_collection(db, name='accounts', class_name='AccountCollection', geo_index=False)
payments = init_edges(db, name='payments', class_name='PaymentEdges')
# token_flow_graph = init_graph(db, 'TokenFlowGraph')

# accounts_list = get_accounts(session)
# accounts.importBulk(accounts_list, onDuplicate='update')
#
max_time = 1577953685
min_time = max_time - 3600*24*30
address = '13wCwuAt5QMymrKXD4ZtEuWmBN53FUDoqN2ov8h1WicCmPbu1Ex'
#
# payments_list = get_recent_payments(session, min_time=min_time, max_time=max_time)
# payments.importBulk(payments_list, onDuplicate='ignore')
#
# payment_totals = get_top_payment_totals(db)
#
# payment_counts = get_top_payment_counts(db)
#
# top_flows_from = get_top_flows_from_accounts(db)
#
# top_flows_to = get_top_flows_to_accounts(db)

balances = get_balance_over_time(session, address, min_time, max_time)