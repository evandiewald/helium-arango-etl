from blockchain_tables import *
import json
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Dict
from datetime import datetime


def get_accounts(session: Session) -> List[Dict]:
    result = session.query(AccountInventory)
    accounts = []
    for row in result.all():
        account = row.as_dict()
        account['_key'] = account['address']
        accounts.append(account)
    return accounts


def get_hotspots(session: Session) -> List[Dict]:
    result = session.query(GatewayInventory, GatewayStatus.online).outerjoin(GatewayStatus, GatewayInventory.address == GatewayStatus.address)
    gateways = []
    for row in result.all():
        (gateway_inventory, status) = row
        gateway = gateway_inventory.as_dict()
        gateway['status'] = status
        gateway['_key'] = gateway['address']
        gateways.append(gateway)
    return gateways


def get_recent_hotspot_rewards(session: Session, address: str, min_time: int, max_time: int, transaction_type: TransactionType = TransactionType.rewards_v1) -> int:
    query = session.query(Rewards.amount, Rewards.gateway, Transactions.type).join(Rewards, Rewards.transaction_hash == Transactions.hash)
    result = query.filter(and_(Rewards.time > min_time, Rewards.time < max_time, Rewards.gateway == address, Transactions.type == transaction_type))
    rewards = []
    for row in result.all():
        rewards.append(row[0])
    return sum(rewards)


def get_recent_payments(session: Session, min_time: int, max_time: int, transaction_type: TransactionType = TransactionType.payment_v1) -> List[Dict]:
    result = session.query(Transactions.fields).filter(and_(Transactions.time > min_time, Transactions.time < max_time, Transactions.type == transaction_type))
    payments = []
    for row in result.all():
        payments.append({'_from': row[0]['payer'], '_to': row[0]['payee'], 'amount': row[0]['amount']})
    return payments


# TODO: parse the outputs to get beaconing features (SNR, RSSI) that are now captured
def get_recent_witnesses(session: Session, address: str, min_time: int, max_time: int, actor_role: TransactionActorRole = TransactionActorRole.challengee):
    min_block = session.query(Blocks.height).filter(Blocks.time > min_time).order_by(Blocks.height).first()[0]
    max_block = session.query(Blocks.height).filter(Blocks.time < max_time).order_by(Blocks.height.desc()).first()[0]
    query = session.query(TransactionActors.actor, Transactions.fields).join(TransactionActors, TransactionActors.transaction_hash == Transactions.hash)
    result = query.filter(and_(TransactionActors.block > min_block, TransactionActors.block < max_block, TransactionActors.actor == address, TransactionActors.actor_role == actor_role))
    witnesses = []
    for row in result.all():
        witnesses.append(row)
    return witnesses


