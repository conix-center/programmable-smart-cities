import sys
import time
import glob
from db import DB
from coords import embed_coords
from draw import render_loop


folder = sys.argv[1]
dbfile = sys.argv[2]
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 4
mapping_file = f"{folder}/mapping.json"
floorplan_file = f"{folder}/floorplan.json"

db = DB(folder, dbfile, limit=limit)
embed_coords(db.g, mapping_file, floorplan_file)

coord_def = """SELECT ?room ?box WHERE {
    ?room brick:hasCoordinates ?box
}"""
db.make_view("coords", coord_def)

temploc_def = """SELECT ?room ?sensor WHERE {
    ?sensor brick:hasLocation ?room .
    ?sensor rdf:type/rdfs:subClassOf* brick:Temperature_Sensor
}"""
db.make_view("temploc", temploc_def)

render_loop(db)
