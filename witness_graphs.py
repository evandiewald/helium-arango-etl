import sys
import time

from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
from config import *
import json
from get_data import *
import math


with open('hotspot_list.json', 'r') as f:
    hotspot_list = json.load(f)


# class Hotspots(object):
class hotspot(Collection):
    _fields = {
        "name": Field()
    }

class witness(Edges):
    _fields = {}


class WitnessGraph(Graph):
    _edgeDefinitions = (EdgeDefinition('witness',
                                       fromCollections=['hotspot'],
                                       toCollections=['hotspot']),)
    _orphanedCollections = []

# def __init__(self, hotspot_list: list):
conn = Connection(username=ARANGO_USERNAME, password=ARANGO_PASSWORD)

db = conn['helium']
if db.hasGraph('WitnessGraph'):
    g = db.graphs['WitnessGraph']
else:
    g = db.createGraph('WitnessGraph')

try:
    hotspot = db['hotspot']
except KeyError:
    hotspot = db.createCollection(className='Collection', name='hotspot')
try:
    witness = db['witness']
except KeyError:
    witness = db.createCollection(className='Edges', name='witness')


address_list = [hotspot['address'] for hotspot in hotspot_list]
CHUNK_SIZE = 500
edge_list = []
print('Generating edge lists...this may take a while')
for chunk in range(math.ceil(len(address_list) / CHUNK_SIZE)):
    chunk_end_idx = min([(chunk+1)*CHUNK_SIZE, len(address_list)])
    edge_list += create_witness_edges_for_address_list(address_list[chunk*CHUNK_SIZE:chunk_end_idx])
    time.sleep(3)
    print((chunk+1)*CHUNK_SIZE, 'hotspots processed...')

# if hotspot.count() < len(hotspot_list):
#     print('Importing hotspot list')
#     hotspot.bulkImport_json('hotspot_list.json', onDuplicate='ignore')

# counter = 29112
# for h in hotspot_list[counter:]:
#     h['_key'] = h['address']
#     # try:
#     _from = 'hotspot/' + h['_key']
#     # except DocumentNotFoundError:
#     #     hotspot.createDocument()
#     witness_list = get_witnesses_for_hotspot(h['address'])
#     existing_edges = [e['_to'] for e in witness.getOutEdges(_from)]
#     for w in witness_list:
#         if 'hotspot/' + w['address'] in existing_edges:
#             continue
#         else:
#             w['_key'] = w['address']
#             try:
#                 _to = 'hotspot/' + hotspot[w['_key']]['_key']
#             except DocumentNotFoundError:
#                 hotspot.createDocument(w).save()
#                 _to = 'hotspot/' + hotspot[w['_key']]['_key']
#             g.link('witness', _from, _to, {})
#     counter += 1
#     if counter % 1000 == 0:
#         print(f'{counter} / {len(hotspot_list)} hotspots processed...')



# Hotspots(hotspot_list)