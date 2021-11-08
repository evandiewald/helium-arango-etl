from pyArango.connection import Connection
from config import *
import json
from pyArango.collection import *
from pyArango.graph import *
import pyArango.validation as VAL
from pyArango.theExceptions import ValidationError
import types
from get_data import *


conn = Connection(username=ARANGO_USERNAME, password=ARANGO_PASSWORD)


class hotspots(Collection):
    _fields = {
        'name': Field()
    }


class witnesses(Edges):
    _fields = {}


class WitnessGraph(Graph):
    _edgeDefinitions = (EdgeDefinition('witnesses',
                                       fromCollections=['hotspots'],
                                       toCollections=['hotspots']),)
    _orphanedCollections = []



try:
    db = conn['helium']
except KeyError:
    db = conn.createDatabase(name='helium')

try:
    hotspots = db['hotspots']
except KeyError:
    hotspots = db.createCollection(name='hotspots')

try:
    witnesses = db['witnesses']
except KeyError:
    witnesses = db.createCollection(name='witnesses', className='Edges')

with open('hotspot_list.json', 'r') as f:
    hotspot_list = json.load(f)

g = db.createGraph(name='WitnessGraph')

for hotspot in hotspot_list[280000:]:
    # hotspot['_key'] = hotspot['address']
    # doc = hotspots.createDocument(hotspot).save()
    witness_list = get_witnesses_for_hotspot(hotspot['address'])
    for witness in witness_list:
        witnesses.createEdge({'_from': 'hotspots/'+hotspot['address'], '_to': 'hotspots/'+witness['address']}).save()



class Witness(COL.Edges):
    _fields = {
        'rssi': COL.Field(),
        'snr': COL.Field()
    }