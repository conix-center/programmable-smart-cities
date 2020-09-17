from flask import Flask, request, json, jsonify, redirect
import logging
import time
import inspect
import threading
from datetime import datetime

app = Flask(__name__, static_url_path='/static')
app.logger.setLevel(logging.INFO)


# 'impls' contains a list of all of the FaultProfile instances that produce
# faults which will be displayed on the dashboard. To add a new fault
# generator, read faults/profile.py and subclass it (see faults/demo.py and
# faults/demo_vavs.py for examples), making sure to implement the required
# method. Instantiate the generator as needed and append to the 'impls' list
impls = []
from faults.demo import DemoFault
impls.append(DemoFault())
from faults.demo_vavs import VAVDemoFault
impls.append(VAVDemoFault(5))

# do one update loop and read from shared status
statuses = []
def update(interval):
    global statuses
    while True:
        statuses = []
        for impl in impls:
            code = inspect.getsource(inspect.getmodule(impl))
            for fault in impl.get_fault_up_until(datetime.now()):
                fault['code'] = code
                statuses.append(fault)
        time.sleep(interval)

@app.route('/', methods=['GET'])
def root():
    return app.send_static_file('index.html')


@app.route('/get_status', methods=['GET'])
def get_status():
    return jsonify(statuses)


if __name__ == '__main__':
    t = threading.Thread(target=update, args=(5,))
    t.start()
    app.run(host='localhost', port='8081')
