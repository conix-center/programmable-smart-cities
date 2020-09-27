from flask import Flask, jsonify
import pandas as pd
import logging
import time
import inspect
import threading
from datetime import datetime

app = Flask(__name__, static_url_path='/static')
app.logger.setLevel(logging.INFO)

# do one update loop and read from shared status
statusLock = threading.Lock()
statuses = []
world_time = datetime.now()


def update(interval):
    global statuses
    global world_time

    # 'impls' contains a list of all of the FaultProfile instances that produce
    # faults which will be displayed on the dashboard. To add a new fault
    # generator, read faults/profile.py and subclass it (see faults/demo.py and
    # faults/demo_vavs.py for examples), making sure to implement the required
    # method. Instantiate the generator as needed and append to the impls list
    impls = []
    from faults.demo import DemoFault
    impls.append(DemoFault())
    from faults.demo_vavs import VAVDemoFault
    impls.append(VAVDemoFault(5))
    from faults.rogue_zone_temp import RogueZoneTemp
    impls.append(RogueZoneTemp("ciee", "ciee"))
    from faults.vav_airflow import VAVAirflow
    impls.append(VAVAirflow()) # TODO: update the initialization here accordingly

    historical_ranges = pd.date_range('2018-01-03', '2019-01-01', freq='24H')
    historical_idx = 0
    while True:
        historical_upper_bound = historical_ranges[historical_idx]
        world_time = historical_upper_bound
        statusLock.acquire()
        statuses = []
        for impl in impls:
            code = inspect.getsource(inspect.getmodule(impl))
            for fault in impl.get_fault_up_until(historical_upper_bound):
                fault['code'] = code
                statuses.append(fault)
        statusLock.release()
        time.sleep(interval)
        historical_idx += 1


@app.route('/', methods=['GET'])
def root():
    return app.send_static_file('index.html')


@app.route('/get_status', methods=['GET'])
def get_status():
    global statuses
    statusLock.acquire()
    print(f"Have {len(statuses)} to render")
    res = jsonify({'statuses': statuses,
                   'time': world_time.strftime('%Y-%m-%d %H:%M:%S')})
    statusLock.release()
    return res


if __name__ == '__main__':
    t = threading.Thread(target=update, args=(1,))
    t.start()
    app.run(host='localhost', port='8081')
