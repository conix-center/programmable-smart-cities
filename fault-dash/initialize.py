from rclient import ReasonableClient
from datadb import Data
import os
#building = 'ciee'
#model_name = 'ciee'

building = 'ebu3b'
model_name = 'ebu3b_mapped'

print("Loading Brick")
c = ReasonableClient("http://localhost:8000")
c.load_file(f"../buildings/{building}/{model_name}.ttl")

print("Loading timeseries")
doreload = not os.path.exists(f"{building}.db")
db = Data(f"../buildings/{building}", f"{building}.db", doreload=doreload)
