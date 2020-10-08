import pandas as pd
import time
import csv
import sys
import gc
from datetime import datetime, timedelta
import sqlite3
import glob
from brickschema.graph import Graph
from brickschema.inference import BrickInferenceSession
from brickschema.namespaces import bind_prefixes, BRICK


def csv2rows(csvfile):
    with open(csvfile, 'r') as fp:
        dr = csv.DictReader(fp)
        to_db = [(i['time'], i['id'], i['value']) for i in dr]
    return to_db


class Data:
    def __init__(self, directory, dbfile, doreload=False):
        self.dbfile = dbfile
        ttl_files = glob.glob(f"{directory}/*.ttl")
        #self.g = Graph(load_brick=False)
        #bind_prefixes(self.g)
        #for ttlf in ttl_files:
        #    print(ttlf)
        #    self.g.load_file(ttlf)
        #self.g = BrickInferenceSession().expand(self.g)

        csv_files = glob.glob(f"{directory}/*.csv")
        self.con = sqlite3.connect(dbfile)
        self.con.row_factory = sqlite3.Row
        # self.con.execute("PRAGMA memory_limit='2GB';")
        if doreload:
            self.con.execute("""CREATE TABLE IF NOT EXISTS data(
                time TIMESTAMP,
                id VARCHAR,
                value REAL
            );""")
            self.con.execute("""CREATE INDEX IF NOT EXISTS data_time_idx \
                                ON data (time, id)""")
            for csvf in csv_files:
                rows = csv2rows(csvf)
                self.con.executemany("""INSERT OR IGNORE INTO data\
                            (time, id, value) VALUES(?, ?, ?)""", rows)
                self.con.commit()
                # self.con.execute(f"COPY data FROM '{csvf}' ( HEADER )")
            self.con.commit()
            print("loaded!")

    def gc(self):
        """
        Cleans up pinned dataframe references, otherwise we hit OOM
        pretty quick
        """
        # self.con.close()
        gc.collect()
        # time.sleep(1)
        # self.con = sqlite3.connect(self.dbfile)

    def view(self, query, header=None):
        df = pd.DataFrame.from_records(self.g.query(query))
        if header is not None:
            df.columns = header
        for col in df.columns:
            df[col] = df[col].astype(str)
        return df

    def data_before(self, dt, uuids=None):
        ts = dt.strftime('%Y-%m-%d %H:%M:%S')
        tsb = (dt - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        uuids = list(uuids.values)
        array = ', '.join(['?'] * len(uuids))
        df = pd.DataFrame.from_records(self.con.execute(f"SELECT time, id, value FROM data WHERE time >= '{tsb}' AND time <= '{ts}' and id IN ({array})", uuids).fetchall())
        if len(df) == 0:
            return pd.DataFrame(columns=['time', 'id', 'value'])
        df.columns = ['time', 'id', 'value']
        # df.columns = [x[0] for x in self.con.description]
        # print(df.head())
        #if uuids is not None:
        #    df = df[df['id'].isin(uuids)]
        return df.set_index(pd.to_datetime(df.pop('time')))

    def filter_type(self, items, bclass):
        """returns all items of the provided class"""
        res = []
        for item in items:
            if self.g.query(f"ASK {{ <{item}> rdf:type/rdfs:subClassOf* <{bclass}> }}")[0]:
                res.append(item)
        return res

    def filter_type2(self, c, items, bclass):
        """returns all items of the provided class"""
        res = []
        for item in items:
            if c.is_type(item, bclass):
                res.append(item)
        return res

def merge_dfs(dfs, value_cols=None):
    for (idx, df) in enumerate(dfs):
        newname = value_cols[idx] if value_cols is not None else f"value_{idx}"
        dfs[idx] = df.rename(columns={'value': newname})
    return pd.concat(dfs)


def find_runs(df, cond):
    #returns ranges of index where 'ser' is True
    rdf = df[cond].copy()
    rdf.loc[:, '_rng'] = cond.astype(int).diff(1).cumsum()
    rdf.loc[:, '_grp'] = cond.astype(int).diff().ne(0).cumsum()
    for (c, (_, idx)) in enumerate(rdf.groupby('_grp')['_rng']):
        if c == 0: continue
        if len(idx) < 2: continue
        yield (idx.index[0], idx.index[-1])

def runs_longer_than(runs, dur):
    for run in runs:
        if (run[-1] - run[0]) > dur:
            yield run


if __name__ == '__main__':
    doreload = len(sys.argv) > 2 and sys.argv[2] == 'reload'
    db = Data("../buildings/ebu3b", sys.argv[1], doreload=doreload)

    from db import ReasonableClient
    c = ReasonableClient("http://localhost:8000")
    # c.load_file("../buildings/ciee/ciee.ttl")
    c.load_file("../buildings/ebu3b/ebu3b_mapped.ttl")

    tspsen = """SELECT ?sensor ?setpoint ?thing ?zone WHERE {
        ?sensor rdf:type brick:Temperature_Sensor .
        ?setpoint rdf:type brick:Temperature_Setpoint .
        ?sensor brick:isPointOf ?thing .
        ?setpoint brick:isPointOf ?thing .
        ?thing brick:controls?/brick:feeds+ ?zone .
        ?zone rdf:type brick:HVAC_Zone
    }"""
    tspsen = c.define_view('tspsen', tspsen)
    print(tspsen)

    # tspsen = db.view(tspsen, header=['sensor', 'setpoint', 'thing', 'zone'])
    for (zone, grp) in tspsen.groupby('zone'):
        sps = grp.pop('setpoint')
        if len(sps.unique()) == 2:
            # use bounds
            grp.loc[:, 'hsp'] = db.filter_type2(c, sps, BRICK['Heating_Temperature_Setpoint'])[0]
            grp.loc[:, 'csp'] = db.filter_type2(c, sps, BRICK['Cooling_Temperature_Setpoint'])[0]
        elif len(sps.unique()) == 1:
            grp.loc[:, 'hsp'] = sps[0]
            grp.loc[:, 'csp'] = sps[0]
        grp = grp.drop_duplicates()
        before = datetime.now()
        sensor_data = db.data_before(before, grp['sensor']).resample('15T').mean()
        hsp_data = db.data_before(before, grp['hsp']).resample('15T').max()
        csp_data = db.data_before(before, grp['csp']).resample('15T').min()
        df = pd.DataFrame()
        df['hsp'] = hsp_data['value']
        df['csp'] = csp_data['value']
        df['temp'] = sensor_data['value']

        for rng in find_runs(df, df['temp'] < df['hsp']):
            print(f"Cold room for {rng}")

        for rng in find_runs(df, df['temp'] > df['csp']):
            print(f"Hot room for {rng}")
