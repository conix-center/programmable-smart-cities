from .profile import FaultProfile
from rclient import ReasonableClient
from datadb import Data, find_runs, runs_longer_than
from brickschema.namespaces import BRICK
import pandas as pd
import os
import time


class RogueZoneTemp(FaultProfile):
    def __init__(self, building, model_name):
        self.c = ReasonableClient("http://localhost:8000")
        # self.c.load_file(f"../buildings/{building}/{model_name}.ttl")

        tspsen = """SELECT ?sensor ?setpoint ?thing ?zone WHERE {
            ?sensor rdf:type brick:Temperature_Sensor .
            ?setpoint rdf:type brick:Temperature_Setpoint .
            ?sensor brick:isPointOf ?thing .
            ?setpoint brick:isPointOf ?thing .
            ?thing brick:controls?/brick:feeds+ ?zone .
            ?zone rdf:type brick:HVAC_Zone .
            FILTER NOT EXISTS { ?thing rdf:type brick:AHU }
        }"""
        self.tspsen = self.c.define_view('tspsen', tspsen)
        doreload = not os.path.exists(f"{building}.db")
        self.db = Data(f"../buildings/{building}", f"{building}.db",
                       doreload=doreload)
        self.grps = {}
        tries = 5
        while tries > 0:
            tries -= 1
            self.tspsen = self.tspsen.refresh()
            if len(self.tspsen) == 0:
                time.sleep(2)
                continue
            for (zone, grp) in self.tspsen.groupby('zone'):
                sps = grp.pop('setpoint')
                if len(sps.unique()) > 1:
                    # use bounds
                    hsps = self.db.filter_type2(self.c, sps, BRICK['Heating_Temperature_Setpoint'])
                    csps = self.db.filter_type2(self.c, sps, BRICK['Cooling_Temperature_Setpoint'])
                    if len(hsps) == 0 or len(csps) == 0:
                        continue
                    grp.loc[:, 'hsp'] = hsps[0]
                    grp.loc[:, 'csp'] = csps[0]
                elif len(sps.unique()) == 1:
                    grp.loc[:, 'hsp'] = sps.values[0]
                    grp.loc[:, 'csp'] = sps.values[0]
                else:
                    continue
                grp = grp.drop_duplicates()
                self.grps[zone] = grp
            break
        super().__init__("RogueZoneTemp")

    def get_fault_up_until(self, upperBound):
        faults = []
        for (zone, grp) in self.grps.items():
            sensor_data = self.db.data_before(upperBound, grp['sensor'])
            hsp_data = self.db.data_before(upperBound, grp['hsp'])
            csp_data = self.db.data_before(upperBound, grp['csp'])
            if not (len(sensor_data) and len(hsp_data) and len(csp_data)):
                continue
            sensor_data = sensor_data.resample('30T').mean()
            hsp_data = hsp_data.resample('30T').min()
            csp_data = csp_data.resample('30T').max()
            df = pd.DataFrame()
            df['hsp'] = hsp_data['value']
            df['csp'] = csp_data['value']
            df['temp'] = sensor_data['value']

            zone_name = zone.split("#")[-1]

            duration_min = pd.to_timedelta('5H')
            cold_spots = list(runs_longer_than(find_runs(df, df['temp'] < df['hsp']), duration_min))
            if len(cold_spots) > 0:
                most_recent = cold_spots[-1]
                dur = most_recent[-1] - most_recent[0]
                faults.append({
                    'name': self.name,
                    'key': f"rogue_zone_{zone_name}",
                    'message': f"Cold zone for {dur}",
                    'last_detected': self.get_timestamp(),
                    'details': {
                        'Zone': zone_name
                    }
                })
                print(zone_name, 'cold fault')

            hot_spots = list(runs_longer_than(find_runs(df, df['temp'] > df['csp']), duration_min))
            if len(hot_spots) > 0:
                most_recent = hot_spots[-1]
                dur = most_recent[-1] - most_recent[0]
                faults.append({
                    'name': self.name,
                    'key': f"rogue_zone_{zone_name}",
                    'message': f"Hot zone for {dur}",
                    'last_detected': self.get_timestamp(),
                    'details': {
                        'Zone': zone_name
                    }
                })
                print(zone_name, 'hot fault')

        return faults
