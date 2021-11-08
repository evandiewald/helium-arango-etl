from sqlalchemy import create_engine
from dotenv import load_dotenv
from blockchain_tables import *
from sqlalchemy.orm import sessionmaker
import json
from blockchain_queries import *
from datetime import datetime
from blockchain_types import *
import os

load_dotenv()

engine = create_engine(os.getenv('POSTGRES_URL'))
Session = sessionmaker(bind=engine)

session = Session()

# get all accounts
# accounts = get_accounts(session)

# get all hotspots
# gateways = get_hotspots(session)

# get recent rewards for a hotspot
address = '113kQU96zqePySTahB7PEde9ZpoWK76DYK1f57wyhjhXCBoAu88'
max_time = 1572655095
min_time = max_time - 60*60*24*30
# rewards = get_recent_hotspot_rewards(session, address, min_time, max_time)

# get recent payments
# payments = get_recent_payments(session, TransactionType.payment_v1, min_time, max_time)

# get recent witnesses for a hotspot
witnesses = get_recent_witnesses(session, address, min_time, max_time)
