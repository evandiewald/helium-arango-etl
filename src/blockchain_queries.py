import sqlalchemy.exc

from blockchain_tables import *
import json
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Dict
from datetime import datetime, timedelta
import h3
from hashlib import md5
from sqlalchemy.engine import Engine


def get_block_by_timestamp(session: Session, timestamp: int) -> int:
    result = session.query(Blocks.height).filter(Blocks.time > timestamp).order_by(Blocks.height).limit(1)
    return result.one()[0]


def get_current_height(session: Session) -> int:
    result = session.query(Blocks.height).order_by(Blocks.height.desc()).limit(1)
    return result.one()[0]


def get_timestamp_by_block(session: Session, height: int) -> int:
    result = session.query(Blocks.time).filter(Blocks.height == height)
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
        # initialize rewards_5d field as null
        gateway['rewards_5d'] = None
        gateways.append(gateway)
    return gateways


def get_hotspot_rewards_by_address(session: Session, address: str, min_time: int, max_time: int, transaction_type: TransactionType = TransactionType.rewards_v1) -> int:
    query = session.query(Rewards.amount, Rewards.gateway, Transactions.type).join(Rewards, Rewards.transaction_hash == Transactions.hash)
    result = query.filter(and_(Rewards.time > min_time, Rewards.time < max_time, Rewards.gateway == address, Transactions.type == transaction_type))
    rewards = []
    for row in result.all():
        rewards.append(row[0])
    return sum(rewards)


def get_hotspot_rewards_overall(engine: Engine, min_time: int, max_time: int) -> List[dict]:
    sql = f"""SELECT gateway, sum(amount) from rewards
    where time > {min_time} and time < {max_time}
    group by gateway
    order by gateway;
    """
    with engine.connect() as conn:
        result = conn.execute(sql)
        rewards = []
        for reward in result.all():
            rewards.append({'address': reward[0], 'rewards': reward[1]})
    return rewards


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


def get_recent_witnesses(session: Session, min_time: int, max_time: int):
    query = session.query(Transactions.time, Transactions.fields)
    # work backwards in time so that we only end up with the most recent version of a given witness path
    result = query.filter(and_(Transactions.time > min_time, Transactions.time < max_time, Transactions.type == 'poc_receipts_v1')).order_by(Transactions.time.desc())
    unique_edges = []
    witnesses = []
    for row in result.all():
        (time, fields) = row
        challengee = fields['path'][0]['challengee']
        for witness in fields['path'][0]['witnesses']:
            # give each path of challengee -> witness a unique hash so that we can simply replace in arango
            edge_hash = md5((challengee+witness['gateway']).encode()).hexdigest()
            if edge_hash not in unique_edges:
                # get all the relevant details and package in arango conventions for edges
                edge = {
                    '_key': edge_hash,
                    '_from': 'hotspots/' + challengee,
                    '_to': 'hotspots/' + witness['gateway'],
                    'time': time
                }
                # flip the order so that we can replace old versions of a witness path with new ones
                witnesses.insert(0, {**edge, **witness}) # Python 3.5+ syntax
                unique_edges.append(edge_hash)
    return witnesses


def get_balances_by_day(engine: Engine,  min_time: int, max_time: int):
    sql = f"""with relevant_blocks as
        (SELECT accounts.address, accounts.balance, accounts.dc_balance, accounts.staked_balance, blocks.time, blocks.timestamp
        from accounts
        INNER JOIN blocks
        ON accounts.block = blocks.height)
        
        SELECT
          address, DATE(timestamp) as balance_date, balance, dc_balance, staked_balance
        FROM
          relevant_blocks
        INNER JOIN
          (SELECT MAX(relevant_blocks.time) AS maxUpdatedAt FROM relevant_blocks GROUP BY DATE(relevant_blocks.timestamp)) as Lookup
            ON Lookup.MaxUpdatedAt = relevant_blocks.time
            where relevant_blocks.time > {min_time} and relevant_blocks.time < {max_time}
            order by address, balance_date;"""
    with engine.connect() as conn:
        result = conn.execute(sql)
        balances = []
        for balance in result.all():
            _balance = {
                'address': balance[0],
                'date': balance[1].isoformat(),
                'balance': balance[2],
                'dc_balance': balance[3],
                'staked_balance': balance[4]
            }
            balances.append(_balance)
    unique_addresses = set(b['address'] for b in balances)
    documents = []
    for unique_address in unique_addresses:
        documents.append({
            '_key': unique_address,
            'daily_balances': list({'date': balance['date'],
                                    'balance': balance['balance'],
                                    'dc_balance': balance['dc_balance'],
                                    'staked_balance': balance['staked_balance']}
                                   for balance in balances if balance['address'] == unique_address)
        })
    return documents

