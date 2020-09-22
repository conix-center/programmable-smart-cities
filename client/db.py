import os
import re
import glob
from brickschema.graph import Graph
from brickschema.inference import BrickInferenceSession
from brickschema.namespaces import bind_prefixes
import csv
import sqlite3
import rdflib

"""
Set up the data from an input folder
"""

def parse_uri(s):
    if " " in s or "[" in s or "]" in s:
        return rdflib.Literal(s)
    elif "http" in s:
        return rdflib.URIRef(s)
    return rdflib.BNode(s)

def csv2rows(csvfile):
    with open(csvfile, 'r') as fp:
        dr = csv.DictReader(fp)
        to_db = [(i['timestamp'], i['id'], i['value']) for i in dr]
    return to_db


class DB:
    def __init__(self, folder, dbfile, limit):
        if not os.path.exists(dbfile) or dbfile == ':memory:':
            ttlfiles = glob.glob(f"{folder}/*.ttl")
            csvfiles = glob.glob(f"{folder}/*/*.csv")

            conn = sqlite3.connect(dbfile)
            conn.row_factory = sqlite3.Row
            for stmt in schema.split(';'):
                stmt += ';'
                conn.execute(stmt)
            conn.commit()

            # load in ttl
            g = Graph()
            bind_prefixes(g)
            for ttl in ttlfiles:
                print(f"Loading TTL file {ttl}")
                g.load_file(ttl)
                # values = list(map(tuple, g))
                # conn.executemany("""INSERT OR IGNORE INTO triples(subject, predicate, object) \
                #                     VALUES(?, ?, ?)""", values)
                # conn.commit()
            g = BrickInferenceSession().expand(g)
            triples = list(g.g)
            conn.executemany("""INSERT OR IGNORE INTO triples(subject, predicate, object) \
                                VALUES(?, ?, ?)""", triples)
            conn.commit()

            # load in data
            for csvf in sorted(csvfiles)[:limit]:
                print(f"Loading CSV file {csvf}")
                rows = csv2rows(csvf)
                #with open(csvf) as f:
                #    rdr = csv.reader(f)
                #    next(rdr) # consume header
                #    vals = list(rdr)
                conn.executemany("""INSERT OR IGNORE INTO data(timestamp, uuid, value) \
                                    VALUES(?, ?, ?)""", rows)
                conn.commit()
        else:
            conn = sqlite3.connect(dbfile)
            conn.row_factory = sqlite3.Row
            # load in ttl
            g = Graph()
            bind_prefixes(g)
            triples = conn.execute("SELECT subject, predicate, object FROM triples")
            for t in triples:
                t = (
                    parse_uri(t[0]),
                    parse_uri(t[1]),
                    parse_uri(t[2]),
                )
                g.add(t)
        self.g = g
        self.conn = conn

    def get_data_bounds(self, uuids):
        """
        Returns a tuple of (max, min) for all the values
        for all the uuids
        """
        with self.conn:
            array = ', '.join(['?'] * len(uuids))
            res = self.conn.execute(f"SELECT MAX(value), MIN(value) FROM \
                data WHERE uuid IN ({array})", uuids)
            return res[0]
            # return list(map(dict, res))

    def sparql(self, query):
        return self.g.query(query)

    def sql(self, query):
        with self.conn:
            res = self.conn.execute(query)
            return list(map(dict, res))

    def get_data(self, uuids):
        with self.conn:
            array = ', '.join(['?'] * len(uuids))
            res = self.conn.execute(f"SELECT timestamp, uuid, value FROM data \
                                    WHERE uuid IN ({array})", uuids)
            return list(map(dict, res))

    def make_view(self, name, sparql):
        res = self.sparql(sparql)
        varlist = re.findall(r'(\?[a-z]+)', sparql.split('WHERE')[0])
        atts = ', '.join([f"{var[1:]} text" for var in varlist])
        inserts = ', '.join([var[1:] for var in varlist])
        insertq = ', '.join(['?' for var in varlist])
        viewdef = f"CREATE TABLE {name} ({atts});"
        insert = f"INSERT INTO {name}({inserts}) VALUES ({insertq})"
        with self.conn:
            self.conn.execute(f"DROP TABLE IF EXISTS {name}")
            self.conn.execute(viewdef)
            self.conn.executemany(insert, map(tuple, res))

schema = """
CREATE TABLE IF NOT EXISTS data (
        timestamp TEXT NOT NULL,
        uuid TEXT NOT NULL,
        value REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS uuid_idx ON data(uuid);
CREATE INDEX IF NOT EXISTS ts_idx ON data(timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS table_idx ON data(timestamp, uuid);

CREATE TABLE IF NOT EXISTS triples (
        subject TEXT NOT NULL,
        predicate TEXT NOT NULL,
        object NOT NULL
);

CREATE VIEW IF NOT EXISTS data_counts AS
        SELECT count(*) as count, uuid FROM data
        GROUP BY uuid ORDER BY count DESC;
"""
