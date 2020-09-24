import pandas as pd
import time
import duckdb
import rdflib
import requests


class View(pd.DataFrame):
    _metadata = ['_view_name', '_view_addr']
    _view_name = 'tmp'
    _view_addr = 'http://localhost:8000'

    @property
    def _constructor(self):
        return View

    def refresh(self):
        resp = requests.get(f"{self._view_addr}/view/{self._view_name}")
        if not resp.ok:
            return self
        d = resp.json()
        v = self.from_records(d['rows'])
        v.columns = d['header']
        v._view_name = self._view_name
        v._view_addr = self._view_addr
        return v


class ReasonableClient:
    def __init__(self, addr):
        self.addr = addr
        self.g = rdflib.Graph()

        # define views
        #all_trips = """SELECT ?s ?p ?o WHERE { ?s ?p ?o }"""
        #self.define_view("all", all_trips)

    def get_view(self, name):
        # resp = requests.get(f"{self.addr}/view/{name}")
        v = View()
        v._view_name = name
        v._view_addr = self.addr
        v = v.refresh()
        return v

    def define_view(self, name, viewdef):
        req = {'name': name, 'query': viewdef}
        resp = requests.post(f"{self.addr}/make", json=req)
        if not resp.ok:
            raise Exception("Could not make view")
        time.sleep(2)
        return self.get_view(name)

    def load_file(self, filename):
        self.g.parse(filename, format=filename.split('.')[-1])
        trips = list(self.g)
        print('num tripes', len(trips))
        rng = list(range(0, len(trips), 2000)) + [len(trips)]
        for x, y in zip(rng[:-1], rng[1:]):
            print(x, y, len(trips[x:y]))
            resp = requests.post(f"{self.addr}/add", json=trips[x:y])
            if not resp.ok:
                raise Exception("Could not add triples", resp.content)

    def is_type(self, item, klass):
        q = f"ASK {{ <{item}> rdf:type <{klass}> }}"
        resp = requests.post(f"{self.addr}/query", json=q)
        return resp.json()[0][0] == 'true'

c = ReasonableClient("http://localhost:8000")
c.load_file("../buildings/ciee/ciee.ttl")

temp_sensor = c.define_view("temp", """SELECT ?sensor WHERE {
    ?sensor rdf:type brick:Temperature_Sensor
}""")


con = duckdb.connect(database='test.db', read_only=False)
con.execute("""DROP TABLE IF EXISTS data;""")
con.execute("""CREATE TABLE data(
    time TIMESTAMP,
    id VARCHAR,
    value REAL
);""")
con.execute("""CREATE INDEX IF NOT EXISTS data_time_idx ON data (time, id)""")
con.execute("COPY data FROM '../buildings/ciee/data/2018-01-01T00:00:00Z.csv' ( HEADER )");
con.execute("COPY data FROM '../buildings/ciee/data/2018-01-02T00:00:00Z.csv' ( HEADER )");
con.execute("COPY data FROM '../buildings/ciee/data/2018-01-03T00:00:00Z.csv' ( HEADER )");
con.commit()

df = con.execute("SELECT * FROM data where time > TIMESTAMP '2018-01-01 12:00:00' and time < TIMESTAMP '2018-01-01 13:00:00'").fetchdf()
df = df.set_index(df.pop('time'))

tspsen = c.define_view("tspsen", """SELECT ?sensor ?setpoint ?thing ?zone WHERE {
    ?sensor rdf:type brick:Temperature_Sensor .
    ?setpoint rdf:type brick:Temperature_Setpoint .
    ?sensor brick:isPointOf ?thing .
    ?setpoint brick:isPointOf ?thing .
    ?thing brick:controls?/brick:feeds+ ?zone .
    ?zone rdf:type brick:HVAC_Zone
}""")
data = tspsen.merge(df, left_on=['sensor'], right_on=['id'])
data = data.merge(df, left_on=['setpoint'], right_on=['id'])
