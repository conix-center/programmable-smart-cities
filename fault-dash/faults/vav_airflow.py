from .profile import FaultProfile
from rclient import ReasonableClient
from datadb import Data, find_runs, runs_longer_than
from brickschema.namespaces import BRICK
import pandas as pd
import os
import time


class VAVAirflow(FaultProfile):
    
    def __init__(self, building):

        self.c = ReasonableClient("http://localhost:8000")

        tspsen = """SELECT ?sensor ?setpoint ?thing ?zone WHERE {
            ?sensor rdf:type brick:Discharge_Air_Flow_Sensor .
            ?setpoint rdf:type brick:Discharge_Air_Flow_Setpoint .
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
                if len(sps.unique()) > 0:
                    afsps = self.db.filter_type2(self.c, sps, BRICK['Discharge_Air_Flow_Setpoint'])
                    if len(afsps) == 0:
                        continue
                    grp.loc[:,'afsp'] = afsps[0]
                else:
                    continue
                grp = grp.drop_duplicates()
                self.grps[zone] = grp
            break

        super().__init__("VAVAirflow")

    def get_fault_up_until(self, upperBound):
        faults = []
        for (zone, grp) in self.grps.items():
            airflow_data = self.db.data_before(upperBound, grp['sensor'])
            afsp_data = self.db.data_before(upperBound, grp['afsp'])
            if not (len(airflow_data) and len(afsp_data)):
                continue

            airflow_std = airflow_data.resample('1h').std()
            airflow_mean = airflow_data.resample('1h').mean()
            afsp_data = afsp_data.resample('1h').mean()

            df = pd.DataFrame()
            df['afstd'] = airflow_std['value']
            df['afmean'] = airflow_mean['value']
            df['vavsp'] = afsp_data['value']

            zone_name = zone.split("#")[-1]

            duration_min = pd.to_timedelta('10T')
            constant_spots = list(find_runs(df, df['afstd'] < 5))
            if len(constant_spots) > 0:
                most_recent = constant_spots[-1]
                dur = most_recent[-1] - most_recent[0]
                dur = strfdelta(dur, "{hours} hours and {minutes} minutes")
                faults.append({
                    'name': self.name,
                    'key': f'constant-zone-{zone_name}', 
                    'message': f"VAV flow suspiciously not changing for {dur}",
                    'last_detected': most_recent[-1],
                    'details': {
                        'Zone': zone_name
                    }
                })
                print(zone_name, "constant vav")

            overflow_spots = list(runs_longer_than(find_runs(df, df['afmean'] > df['vavsp']), duration_min))
            if len(constant_spots) > 0:
                most_recent = constant_spots[-1]
                dur = most_recent[-1] - most_recent[0]
                dur = strfdelta(dur, "{hours} hours and {minutes} minutes")
                faults.append({
                    'name': self.name,
                    'key': f'constant-zone-{zone_name}', 
                    'message': f"VAV overflowing (airflow greater than setpoint) for {dur}",
                    'last_detected': most_recent[-1],
                    'details': {
                        'Zone': zone_name
                    }
                })
                print(zone_name, "overflowing vav")


        return faults


def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)