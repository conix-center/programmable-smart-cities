import sys
from db import DB
from coords import embed_coords
from draw import render_loop


if len(sys.argv) < 5:
    print("Usage: python main.py <building folder>\
<database file> <scene name> <quadrant>")
    sys.exit(0)
folder = sys.argv[1]
dbfile = sys.argv[2]
scene = sys.argv[3]
quadrant = int(sys.argv[4])

mapping_file = f"{folder}/mapping.json"
floorplan_file = f"{folder}/floorplan.json"

db = DB(folder, dbfile, limit=None)
embed_coords(db.g, mapping_file, floorplan_file)

coord_def = """SELECT ?room ?box WHERE {
    ?room brick:hasCoordinates ?box
}"""
db.make_view("coords", coord_def)

temploc_def = """SELECT ?room ?sensor WHERE {
    ?sensor brick:hasLocation|(brick:isPointOf/brick:feeds/brick:hasPart) ?room .
    ?sensor rdf:type brick:Temperature_Sensor
}"""
db.make_view("temploc", temploc_def)

render_loop(scene, db, quadrant)
