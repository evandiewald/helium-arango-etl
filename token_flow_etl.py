from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
from dotenv import load_dotenv
import json
import math
import os


# check if arango exists and is properly initialized, extract data from db, package into arango-friendly format, bulk import
# run in loop
# make a collection for latest updates? or just include as a field