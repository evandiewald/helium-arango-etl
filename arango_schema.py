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


class HotspotCollection(COL.Collection):

    def ensureGeoJsonIndex(self, fields, name = None, geoJson = False):
        """Creates a geo index if it does not already exist, and returns it."""
        data = {
            "type" : "geo",
            "fields" : fields,
            "geoJson" : geoJson
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["geo"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

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
        'geo_location': COL.Field()
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


# class TokenFlowGraph(Graph):

    # _edgeDefinitions =
    # _orphanedCollections = []