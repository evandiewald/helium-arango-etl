import pyArango.collection as COL
import pyArango.validation as VAL
from pyArango.index import Index
import json
from blockchain_types import *
from pyArango.graph import Graph, EdgeDefinition


_validation_base = {
        'on_save': True,
        'on_set': True,
        'allow_foreign_fields': True
    }


def ensureGeoJsonIndex(collection: COL.Collection, fields, name=None, geoJson=False):
    """Creates a geo index if it does not already exist, and returns it."""
    data = {
        "type": "geo",
        "fields": fields,
        "geoJson": geoJson
    }
    if name:
        data["name"] = name
    ind = Index(collection, creationData=data)
    collection.indexes["geo"][ind.infos["id"]] = ind
    if name:
        collection.indexes_by_name[name] = ind
    return ind


class HotspotCollection(COL.Collection):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'address': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'owner': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'location': COL.Field(validators=[VAL.String()]),
        'last_poc_challenge': COL.Field(validators=[VAL.Int()]),
        'last_poc_onion_key_hash': COL.Field(validators=[VAL.String()]),
        'witnesses': COL.Field(),
        'first_block': COL.Field(validators=[VAL.Int()]),
        'last_block': COL.Field(validators=[VAL.Int()]),
        'nonce': COL.Field(validators=[VAL.Int()]),
        'name': COL.Field(validators=[VAL.String()]),
        'first_timestamp': COL.Field(),
        'reward_scale': COL.Field(validators=[VAL.Numeric()]),
        'elevation': COL.Field(validators=[VAL.Int()]),
        'gain': COL.Field(validators=[VAL.Int()]),
        'location_hex': COL.Field(validators=[VAL.String()]),
        'mode': COL.Field(validators=[VAL.Enumeration(GatewayMode)]),
        'payer': COL.Field(validators=[VAL.String()]),
        'geo_location': COL.Field(),
        'rewards_5d': COL.Field(validators=[VAL.Int()]),
        'betweenness_centrality': COL.Field(validators=[VAL.Numeric()]),
        'pagerank': COL.Field(validators=[VAL.Numeric()]),
        'hub_score': COL.Field(validators=[VAL.Numeric()]),
        'authority_score': COL.Field(validators=[VAL.Numeric()])
    }


class AccountCollection(COL.Collection):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'address': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'dc_balance': COL.Field(validators=[VAL.Int()]),
        'dc_nonce': COL.Field(validators=[VAL.Int()]),
        'security_balance': COL.Field(validators=[VAL.Int()]),
        'balance': COL.Field(validators=[VAL.Int()]),
        'nonce': COL.Field(validators=[VAL.Int()]),
        'first_block': COL.Field(validators=[VAL.Int()]),
        'last_block': COL.Field(validators=[VAL.Int()]),
        'staked_balance': COL.Field(validators=[VAL.Int()])
    }


class PaymentEdges(COL.Edges):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        '_from': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        '_to': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'amount': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'time': COL.Field(validators=[VAL.NotNull(), VAL.Int()])
    }


class BalancesCollection(COL.Collection):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'daily_balances': COL.Field()
    }


class CitiesCollection(COL.Collection):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'city_id': COL.Field(),
        'long_city': COL.Field(),
        'long_state': COL.Field(),
        'long_country': COL.Field(),
    }


class WitnessEdges(COL.Edges):

    _validation = _validation_base

    _fields = {
        '_key': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        '_from': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        '_to': COL.Field(validators=[VAL.NotNull(), VAL.String()]),
        'snr': COL.Field(validators=[VAL.NotNull(), VAL.Numeric()]),
        'frequency': COL.Field(validators=[VAL.NotNull(), VAL.Numeric()]),
        'signal': COL.Field(validators=[VAL.NotNull(), VAL.Int()]),
        'time': COL.Field(validators=[VAL.NotNull(), VAL.Int()]),
        'datarate': COL.Field(validators=[VAL.String()]),
        'location': COL.Field(validators=[VAL.String()]),
        'timestamp': COL.Field(validators=[VAL.NotNull(), VAL.Int()]),
    }
