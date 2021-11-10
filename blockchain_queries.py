import sqlalchemy.exc

from blockchain_tables import *
import json
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Dict
from datetime import datetime, timedelta
import h3
from hashlib import md5


def get_block_by_timestamp(session: Session, timestamp: int) -> int:
    result = session.query(Blocks.height).filter(Blocks.time > timestamp).order_by(Blocks.height).limit(1)
    return result.one()[0]


def timestamp_for_end_of_day(timestamp: int) -> int:
    """Given a timestamp, return the UTC timestamp of midnight on that day"""
    this_time_tm = datetime.fromtimestamp(timestamp) + timedelta(days=1)
    return int(datetime.timestamp(this_time_tm - timedelta(hours=this_time_tm.hour, minutes=this_time_tm.minute, seconds=this_time_tm.second)))


def get_accounts(session: Session) -> List[Dict]:
    result = session.query(AccountInventory)
    accounts = []
    for row in result.all():
        account = row.as_dict()
        account['_key'] = account['address']
        accounts.append(account)
    return accounts


def get_hotspots(session: Session, include_key: bool = True, h3_to_geo: bool = True) -> List[Dict]:
    result = session.query(GatewayInventory, GatewayStatus.online).outerjoin(GatewayStatus, GatewayInventory.address == GatewayStatus.address)
    gateways = []
    for row in result.all():
        (gateway_inventory, status) = row
        gateway = gateway_inventory.as_dict()
        gateway['status'] = status
        if include_key:
            gateway['_key'] = gateway['address']
        if h3_to_geo:
            try:
                gateway['geo_location'] = {'coordinates': h3.h3_to_geo(gateway['location_hex'])[::-1], 'type': 'Point'}
            except TypeError:
                gateway['geo_location'] = {'coordinates': None, 'type': 'Point'}
        gateways.append(gateway)
    return gateways


def get_hotspot_rewards(session: Session, address: str, min_time: int, max_time: int, transaction_type: TransactionType = TransactionType.rewards_v1) -> int:
    query = session.query(Rewards.amount, Rewards.gateway, Transactions.type).join(Rewards, Rewards.transaction_hash == Transactions.hash)
    result = query.filter(and_(Rewards.time > min_time, Rewards.time < max_time, Rewards.gateway == address, Transactions.type == transaction_type))
    rewards = []
    for row in result.all():
        rewards.append(row[0])
    return sum(rewards)


def get_recent_payments(session: Session, min_time: int, max_time: int, transaction_type: TransactionType = TransactionType.payment_v1) -> List[Dict]:
    result = session.query(Transactions.fields, Transactions.time).filter(and_(Transactions.time > min_time, Transactions.time < max_time, Transactions.type == transaction_type))
    payments = []
    for row in result.all():
        # need a unique key so that this payment is not double-counted
        payment_hash = md5(json.dumps(row[0]).encode()).hexdigest()
        payments.append({'_key': payment_hash,
                         '_from': 'accounts/' + row[0]['payer'],
                         '_to': 'accounts/' + row[0]['payee'],
                         'amount': row[0]['amount'],
                         'time': row[1]})
    return payments


# TODO: parse the outputs to get beaconing features (SNR, RSSI) that are now captured
def get_recent_witnesses(session: Session, address: str, min_time: int, max_time: int, actor_role: TransactionActorRole = TransactionActorRole.challengee):
    # min_block = session.query(Blocks.height).filter(Blocks.time > min_time).order_by(Blocks.height).first()[0]
    # max_block = session.query(Blocks.height).filter(Blocks.time < max_time).order_by(Blocks.height.desc()).first()[0]
    min_block, max_block = get_block_by_timestamp(session, min_time), get_block_by_timestamp(session, max_time)
    query = session.query(TransactionActors.actor, Transactions.fields).join(TransactionActors, TransactionActors.transaction_hash == Transactions.hash)
    result = query.filter(and_(TransactionActors.block > min_block, TransactionActors.block < max_block, TransactionActors.actor == address, TransactionActors.actor_role == actor_role))
    witnesses = []
    for row in result.all():
        # make sure to add unique _key to each to prevent duplicates
        witnesses.append(row)
    return witnesses


def get_balance_over_time(session: Session, address: str, min_time: int, max_time: int):
    query = session.query(Accounts.balance, Blocks.timestamp).join(Accounts, Accounts.block == Blocks.height)
    results = query.filter(and_(Blocks.time > min_time, Blocks.time < max_time, Accounts.address == address)).order_by(Blocks.time)
    balances = []
    for balance in results.all():
        balances.append({
            'time': int(balance[1].timestamp()),
            'balance': balance[0]
        })
    return balances

