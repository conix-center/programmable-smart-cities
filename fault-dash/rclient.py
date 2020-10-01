import pandas as pd
import rdflib
import requests

#### IMPORTANT
# Run the binary downloaded from here which is the backing server
# for this client https://github.com/gtfierro/reasonable/releases/tag/conix-v1


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

    def get_view(self, name):
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
        return self.get_view(name)

    def load_file(self, filename):
        self.g.parse(filename, format=filename.split('.')[-1])
        trips = list(self.g)
        print(f"Loading {len(trips)} triples")
        rng = list(range(0, len(trips), 5000)) + [len(trips)]
        for x, y in zip(rng[:-1], rng[1:]):
            print(x, y, len(trips[x:y]))
            # choose nonblocking endpoint if we are bulk loading
            endpoint = "addnb" if y!=len(trips) else "add"
            url = f"{self.addr}/{endpoint}"
            resp = requests.post(url, json=trips[x:y])
            if not resp.ok:
                raise Exception("Could not add triples", resp.content)

    def is_type(self, item, klass):
        q = f"ASK {{ <{item}> rdf:type/rdfs:subClassOf* <{klass}> }}"
        resp = requests.post(f"{self.addr}/query", json=q)
        return resp.json()[0][0] == 'true'


if __name__ == '__main__':
    c = ReasonableClient("http://localhost:8000")

    temp_sensor = c.define_view("temp", """SELECT ?sensor WHERE {
        ?sensor rdf:type brick:Temperature_Sensor
    }""")

    tspsen = c.define_view("tspsen", """SELECT ?sensor ?setpoint ?thing ?zone
    WHERE {
        ?sensor rdf:type brick:Temperature_Sensor .
        ?setpoint rdf:type brick:Temperature_Setpoint .
        ?sensor brick:isPointOf ?thing .
        ?setpoint brick:isPointOf ?thing .
        ?thing brick:controls?/brick:feeds+ ?zone .
        ?zone rdf:type brick:HVAC_Zone
    }""")
