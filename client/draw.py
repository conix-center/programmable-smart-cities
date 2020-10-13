import paho.mqtt.client as mqtt
import time
import json
import pandas as pd

UNIVERSE = "realm"
BROKER = "arena.andrew.cmu.edu"
c = mqtt.Client()
c.connect(BROKER)


def get_rooms(db):
    rooms = {}

    res = db.sql("SELECT room, box FROM coords")
    for row in res:
        room = row['room']
        box = row['box']
        if box.startswith('#'):
            box = box[1:]
        box = json.loads(box)
        x1, y1 = min(box[0][0], box[1][0]), min(box[0][1], box[1][1])
        x2, y2 = max(box[0][0], box[1][0]), max(box[0][1], box[1][1])

        rooms[room] = {
            'box': [(x1, y1), (x2, y2)],
            'room': str(room),
            'sensor': []
        }
    res = db.sql('SELECT temploc.room, sensor \
                  FROM temploc \
                  JOIN coords \
                    ON temploc.room = coords.room')
    for row in res:
        room = row['room']
        sensor = row['sensor']
        roomdef = rooms.get(room, {'sensor': []})
        roomdef['sensor'].append(sensor)

    return list(rooms.values())


def render_boxes(scene, rooms, quadrant):
    if quadrant == 1:
        xoff, zoff = -16, -16
    elif quadrant == 2:
        xoff, zoff = -40, 0
    elif quadrant == 3:
        xoff, zoff = -15, 15
    elif quadrant == 4:
        xoff, zoff = -12, 12
    for r in rooms:
        print(r)
        # min_x = min(r['box'][0][0], r['box'][1][0])
        # min_z = min(r['box'][0][1], r['box'][1][1])
        mid_x = (r['box'][0][0] + r['box'][1][0]) / 2
        mid_z = (r['box'][0][1] + r['box'][1][1]) / 2
        x_size = max(abs(r['box'][0][0] - r['box'][1][0]), 1)
        z_size = max(abs(r['box'][0][1] - r['box'][1][1]), 1)
        o = {
            "object_id": r['room'].split('#')[-1],
            "action": "create",
            "persist": True,
            "type": "object",
            "data": {
                "object_type": "cube",
                "position": {"x": xoff+mid_x, "y": 0, "z": zoff+mid_z},
                "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
                "scale": {"x": x_size, "y": 1, "z": z_size},
                "color": f"#ffffff"
            }
        }
        text = {"object_id" : f"label_{o['object_id']}",
                "action": "create",
                "persist": True,
                "data": {
                    "color": "white",
                    "text": r['room'].split('#')[-1],
                    "object_type": "text",
                    "position": {"x": xoff+mid_x, "y": 1, "z": zoff+mid_z},
                    "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
                    "scale": {"x": 1, "y": 1, "z": 1}}}
        render(scene, text)
        render(scene, o)

def render(scene, o):
    topic = f"{UNIVERSE}/s/{scene}/{o['object_id']}"
    # print(topic)
    # print(json.dumps(o))
    x = c.publish(topic, json.dumps(o))
    # print(x)


def temp_to_color(v, maxval=80, minval=60):
    mp = (maxval + minval) / 2.
    diff = (maxval - mp)
    redness = 255 + int(255 * (min((v-mp)/diff, 0)))
    blueness = 255 + int(255 * (min((mp-v)/diff, 0)))
    # being closer to the midpoint should give us purple
    return f"{redness:02x}11{blueness:02x}"


def get_data(db, rooms):
    uuids = [x for r in rooms for x in r['sensor']]
    print(uuids)
    resp = db.get_data(uuids)
    df = pd.DataFrame.from_records(resp)
    print(df.head())
    df = df.set_index(pd.to_datetime(df.pop('timestamp')))
    df = df.groupby('uuid').resample('60T').mean().reset_index()
    df = df.set_index(df.pop('timestamp'))
    df = df.rename(columns={'uuid': 'sensor'})
    return df.dropna()

def render_loop(scene, db, quadrant):
    rooms = get_rooms(db)
    render_boxes(scene, rooms, quadrant)
    print(f"View on https://arena.andrew.cmu.edu/?scene={scene}")
    df = get_data(db, rooms)
    maxb, minb = df['value'].max(), df['value'].min()
    while True:
        print(f"Looping through data", maxb, minb)
        for ts in df.index:
            print(">>>>>>>>>>>>>>", ts)
            for row in df.loc[ts].iterrows():
                sensor = row[1]['sensor']
                for room in rooms:
                    if sensor in room['sensor']:
                        room = room['room']
                        break
                value = row[1]['value']
                color = temp_to_color(value, maxval=maxb, minval=minb)
                print(sensor, value, color)
                o = {
                    "object_id": room.split('#')[-1],
                    "action": "update",
                    "type": "object",
                    "data": {
                        "material": {"color": f"#{color}"},
                    }
                }
                render(scene, o)
            time.sleep(2)
