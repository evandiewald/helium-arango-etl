from sqlalchemy import Table, Column, Integer, String, JSON, MetaData, Text, BigInteger, Enum, DateTime
import enum
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, DOUBLE_PRECISION, TIMESTAMP
import json
from typing import Dict
from blockchain_types import *
from geoalchemy2 import Geometry


Base = declarative_base()


class Accounts(Base):
    __tablename__ = 'accounts'

    block = Column('block', BigInteger(), primary_key=True, nullable=False)
    address = Column('address', Text(), primary_key=True, nullable=False)
    dc_balance = Column('dc_balance', BigInteger(), nullable=False)
    dc_nonce = Column('dc_nonce', BigInteger(), nullable=False)
    security_balance = Column('security_balance', BigInteger(), nullable=False)
    balance = Column('balance', BigInteger(), nullable=False)
    nonce = Column('nonce', BigInteger(), nullable=False)
    staked_balance = Column('staked_balance', BigInteger(), nullable=False)

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class AccountInventory(Base):
    __tablename__ = 'account_inventory'

    address = Column('address', Text(), primary_key=True, nullable=False)
    dc_balance = Column('dc_balance', BigInteger(), nullable=False)
    dc_nonce = Column('dc_nonce', BigInteger(), nullable=False)
    security_balance = Column('security_balance', BigInteger(), nullable=False)
    balance = Column('balance', BigInteger(), nullable=False)
    nonce = Column('nonce', BigInteger(), nullable=False)
    first_block = Column('first_block', BigInteger())
    last_block = Column('last_block', BigInteger())
    staked_balance = Column('staked_balance', BigInteger(), nullable=False)

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class TransactionActors(Base):
    __tablename__ = 'transaction_actors'

    actor = Column('actor', Text(), primary_key=True, nullable=False)
    actor_role = Column('actor_role', Enum(TransactionActorRole), primary_key=True, nullable=False)
    transaction_hash = Column('transaction_hash', Text(), nullable=False)
    block = Column('block', BigInteger(), nullable=False)

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class Transactions(Base):
    __tablename__ = 'transactions'

    block = Column('block', BigInteger(), primary_key=False, nullable=False)
    hash = Column('hash', Text(), primary_key=True, nullable=False)
    type = Column('type', Enum(TransactionType), nullable=False)
    fields = Column('fields', JSONB())
    time = Column('time', BigInteger())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class Gateways(Base):
    __tablename__ = 'gateways'

    block = Column('block', BigInteger(), primary_key=True, nullable=False)
    address = Column('address', Text(), primary_key=True, nullable=False)
    owner = Column('owner', Text())
    location = Column('location', Text())
    last_poc_challenge = Column('last_poc_challenge', BigInteger())
    last_poc_onion_key_hash = Column('last_poc_onion_key_hash', Text())
    witnesses = Column('witnesses', JSONB())
    nonce = Column('nonce', BigInteger())
    name = Column('name', Text())
    time = Column('time', BigInteger())
    reward_scale = Column('reward_scale', DOUBLE_PRECISION())
    elevation = Column('elevation', Integer())
    gain = Column('gain', Integer())
    location_hex = Column('location_hex', Text())
    mode = Column('mode', Enum(GatewayMode))

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class GatewayInventory(Base):
    __tablename__ = 'gateway_inventory'

    address = Column('address', Text(), primary_key=True, nullable=False)
    owner = Column('owner', Text())
    location = Column('location', Text())
    last_poc_challenge = Column('last_poc_challenge', BigInteger())
    last_poc_onion_key_hash = Column('last_poc_onion_key_hash', Text())
    witnesses = Column('witnesses', JSONB())
    first_block = Column('first_block', BigInteger())
    last_block = Column('last_block', BigInteger())
    nonce = Column('nonce', BigInteger())
    name = Column('name', Text())
    first_timestamp = Column('first_timestamp', DateTime(timezone=True))
    reward_scale = Column('reward_scale', DOUBLE_PRECISION())
    elevation = Column('elevation', Integer())
    gain = Column('gain', Integer())
    location_hex = Column('location_hex', Text())
    mode = Column('mode', Enum(GatewayMode))
    payer = Column('payer', Text())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class GatewayStatus(Base):
    __tablename__ = 'gateway_status'

    address = Column('address', Text(), primary_key=True, nullable=False)
    online = Column('online', Text())
    block = Column('first_block', BigInteger())
    updated_at = Column('updated_at', DateTime(timezone=True))
    poc_interval = Column('poc_interval', BigInteger())
    peer_timestamp = Column('peer_timestamp', DateTime(timezone=True), nullable=True)
    listen_addrs = Column('listen_addrs', JSONB())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class Rewards(Base):
    __tablename__ = 'rewards'

    block = Column('block', BigInteger(), primary_key=True, nullable=False)
    transaction_hash = Column('transaction_hash', Text())
    time = Column('time', BigInteger())
    account = Column('account', Text(), primary_key=True, nullable=False)
    gateway = Column('gateway', Text(), primary_key=True, nullable=False)
    amount = Column('amount', BigInteger())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class DCBurns(Base):
    __tablename__ = 'dc_burns'

    block = Column('block', BigInteger())
    transaction_hash = Column('transaction_hash', Text(), primary_key=True)
    actor = Column('actor', Text(), primary_key=True, nullable=False)
    type = Column('type', Text(), primary_key=True, nullable=False)
    amount = Column('amount', BigInteger())
    oracle_price = Column('oracle_price', BigInteger())
    time = Column('time', BigInteger())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class Blocks(Base):
    __tablename__ = 'blocks'

    height = Column('height', BigInteger(), primary_key=True, nullable=False)
    time = Column('time', BigInteger())
    timestamp = Column('timestamp', DateTime(timezone=True))
    prev_hash = Column('prev_hash', Text())
    block_hash = Column('block_hash', Text())
    transaction_count = Column('transaction_count', Integer())
    hbbft_round = Column('hbbft_round', BigInteger())
    election_epoch = Column('election_epoch', BigInteger())
    epoch_start = Column('epoch_start', BigInteger())
    rescue_signature = Column('rescue_signature', Text())
    snapshot_hash = Column('snapshot_hash', Text())
    created_at = Column('created_at', DateTime(timezone=True))

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


class Locations(Base):
    __tablename__ = 'locations'

    location = Column('location', Text(), primary_key=True, nullable=False)
    long_street = Column('long_street', Text())
    short_street = Column('short_street', Text())
    long_city = Column('long_city', Text())
    short_city = Column('short_city', Text())
    long_state = Column('long_state', Text())
    short_state = Column('short_state', Text())
    long_country = Column('long_country', Text())
    short_country = Column('short_country', Text())
    city_id = Column('city_id', Text())

    def as_dict(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


