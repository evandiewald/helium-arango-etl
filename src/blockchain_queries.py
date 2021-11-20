import sqlalchemy.exc
from blockchain_tables import *
import json
from sqlalchemy.orm import Session, Query
from sqlalchemy import and_
from sqlalchemy.sql import func
from typing import List, Dict, Union
from datetime import datetime, timedelta
import h3
from hashlib import md5
from sqlalchemy.engine import Engine


def get_result_batch(query: Query, slice_start: int, slice_end: int):
    return query.slice(slice_start, slice_end)


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
    """Gets all accounts in a single list. Batched methods are preferable at scale."""
    result = session.query(AccountInventory)
    accounts = []
    for row in result.all():
        account = row.as_dict()
        account['_key'] = account['address']
        accounts.append(account)
    return accounts


class BatchedQuery(object):
    def __init__(self, batch_size: int, query: Union[Query, str]):
        self.query = query
        self.batch_size = batch_size
        self.slice_start = 0
        self.slice_end = batch_size
        self.query_complete = False

    def _update_slice(self):
        self.slice_start = self.slice_end
        self.slice_end += self.batch_size

    def get_next_batch(self) -> Union[List[Dict], List]:
        pass


class AccountInventoryBatchedQuery(BatchedQuery):
    def __init__(self, session: Session, batch_size: int):
        query = session.query(AccountInventory)
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        accounts = []
        for row in self.query.slice(self.slice_start, self.slice_end):
            account = row.as_dict()
            account['_key'] = account['address']
            accounts.append(account)
        if len(accounts) == 0:
            self.query_complete = True
        else:
            self._update_slice()
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
        # initialize extra fields as null
        gateway['rewards_5d'], gateway['betweenness_centrality'], gateway['pagerank'], gateway['hub_score'], gateway['authority_score'] = None, None, None, None, None
        gateways.append(gateway)
    return gateways


class GatewayInventoryBatchedQuery(BatchedQuery):
    def __init__(self, session: Session, batch_size: int):
        query = session.query(GatewayInventory, GatewayStatus.online).outerjoin(GatewayStatus, GatewayInventory.address == GatewayStatus.address)
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        gateways = []
        for row in self.query.slice(self.slice_start, self.slice_end):
            (gateway_inventory, status) = row
            gateway = gateway_inventory.as_dict()
            gateway['status'] = status
            gateway['_key'] = gateway['address']
            try:
                gateway['geo_location'] = {'coordinates': h3.h3_to_geo(gateway['location_hex'])[::-1], 'type': 'Point'}
            except TypeError:
                gateway['geo_location'] = {'coordinates': None, 'type': 'Point'}
            # initialize extra fields as null
            gateway['rewards_5d'], gateway['betweenness_centrality'], gateway['pagerank'], gateway['hub_score'], gateway[
                'authority_score'] = None, None, None, None, None
            gateways.append(gateway)
        if len(gateways) == 0:
            self.query_complete = True
        else:
            self._update_slice()
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


class GatewayRewardsBatchedQuery(BatchedQuery):
    def __init__(self, session: Session, batch_size: int, min_time: int, max_time: int):
        query = session.query(Rewards.gateway, func.sum(Rewards.amount)).where(and_(Rewards.time > min_time, Rewards.time < max_time)).group_by(Rewards.gateway).order_by(Rewards.gateway)
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        rewards = []
        for reward in self.query.slice(self.slice_start, self.slice_end):
            rewards.append({'address': reward[0], 'rewards': reward[1]})
        if len(rewards) == 0:
            self.query_complete = True
        else:
            self._update_slice()
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


class RecentPaymentsBatchedQuery(BatchedQuery):
    def __init__(self, session: Session, batch_size: int, min_time: int, max_time: int):
        query = session.query(Transactions.fields, Transactions.time).filter(and_(Transactions.time > min_time, Transactions.time < max_time, Transactions.type.in_(('payment_v1', 'payment_v2'))))
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        payments = []
        for row in self.query.slice(self.slice_start, self.slice_end):
            # need a unique key so that this payment is not double-counted
            payment_hash = md5(json.dumps(row[0]).encode()).hexdigest()
            try:
                # payment_v1 structure
                payments.append({'_key': payment_hash,
                                 '_from': 'accounts/' + row[0]['payer'],
                                 '_to': 'accounts/' + row[0]['payee'],
                                 'amount': row[0]['amount'],
                                 'time': row[1]})
            except KeyError:
                # payment_v2 slightly different
                payments.append({'_key': payment_hash,
                                 '_from': 'accounts/' + row[0]['payer'],
                                 '_to': 'accounts/' + row[0]['payments'][0]['payee'],
                                 'amount': row[0]['payments'][0]['amount'],
                                 'time': row[1]})
        if len(payments) == 0:
            self.query_complete = True
        else:
            self._update_slice()
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


class RecentWitnessesBatchedQuery(BatchedQuery):
    def __init__(self, session: Session, batch_size: int, min_time: int, max_time: int):
        query1 = session.query(Transactions.time, Transactions.fields)
        # work backwards in time so that we only end up with the most recent version of a given witness path
        query = query1.filter(and_(Transactions.time > min_time, Transactions.time < max_time, Transactions.type == 'poc_receipts_v1')).order_by(
            Transactions.time.desc())
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        unique_edges = []
        witnesses = []
        for row in self.query.slice(self.slice_start, self.slice_end):
            (time, fields) = row
            challengee = fields['path'][0]['challengee']
            for witness in fields['path'][0]['witnesses']:
                # give each path of challengee -> witness a unique hash so that we can simply replace in arango
                edge_hash = md5((challengee + witness['gateway']).encode()).hexdigest()
                if edge_hash not in unique_edges:
                    # get all the relevant details and package in arango conventions for edges
                    edge = {
                        '_key': edge_hash,
                        '_from': 'hotspots/' + challengee,
                        '_to': 'hotspots/' + witness['gateway'],
                        'time': time
                    }
                    # flip the order so that we can replace old versions of a witness path with new ones
                    witnesses.insert(0, {**edge, **witness})  # Python 3.5+ syntax
                    unique_edges.append(edge_hash)
        if len(witnesses) == 0:
            self.query_complete = True
        else:
            self._update_slice()
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


class DailyBalancesBatchedQuery(BatchedQuery):
    def __init__(self, engine: Engine, batch_size: int, min_time: int, max_time: int):
        query = """with relevant_blocks as
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
                    where relevant_blocks.time > {0} and relevant_blocks.time < {1}
                    order by address, balance_date
                    limit {2}
                    offset {3};"""
        self.engine = engine
        self.min_time = min_time
        self.max_time = max_time
        super().__init__(batch_size, query)

    def get_next_batch(self) -> Union[List[Dict], List]:
        with self.engine.connect() as conn:
            query = self.query.format(self.min_time, self.max_time, self.batch_size, self.slice_start)
            result = conn.execute(query)
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
        if len(documents) == 0:
            self.query_complete = True
        else:
            self._update_slice()
        return documents