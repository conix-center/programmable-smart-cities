from rclient import ReasonableClient
from datadb import Data
import yaml
import sys
import os

if len(sys.argv) < 2:
    print("Usage: python initialize.py <config file>")
    sys.exit(1)
cfg = yaml.load(open(sys.argv[1]))
building = cfg['building_name']
ttl_file = cfg['ttl_file']

print("Loading Brick")
c = ReasonableClient("http://localhost:8000")
c.load_file(ttl_file)

print("Loading timeseries")
doreload = not os.path.exists(f"{building}.db")
db = Data(cfg["timeseries_files"], f"{building}.db", doreload=doreload)
