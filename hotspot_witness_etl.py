from blockchain_queries import *
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import math
import os
from arango_queries import *
from pyArango.connection import *

# check if arango exists and is properly initialized, extract data from db, package into arango-friendly format, bulk import
# run in loop
# make a collection for latest updates? or just include as a field

load_dotenv()

#### ARANGO CONFIG ####
arango_connection = Connection(
    arangoURL=os.getenv('ARANGO_URL'),
    username=os.getenv('ARANGO_USERNAME'),
    password=os.getenv('ARANGO_PASSWORD')
)
#### POSTGRES CONFIG ####
postgres_engine = create_engine(os.getenv('POSTGRES_URL'))
Session = sessionmaker(bind=postgres_engine)
session = Session()


db = init_database(arango_connection, 'helium')

hotspots = init_collection(db, name='hotspots', class_name='HotspotCollection', geo_index=True)

hotspot_list = get_hotspots(session, include_key=True, h3_to_geo=True)

hotspots.importBulk(hotspot_list, onDuplicate='update')