from brickschema.namespaces import BRICK
import rdflib
import json

def get_vertex_coords(storey, ident):
    for vertex in storey['geometry']['vertices']:
        if vertex['id'] == ident:
            return (vertex['x'], vertex['y'])


def get_coords(floorplan_data, name):
    coords = set()
    print(f"Searching coords for {name}")
    for storey in floorplan_data['stories']:
        for space in storey['spaces']:
            if space['name'] == name:
                break
            space = None
        if space is None:
            continue
        # print(f"Using space {space}")
        for face in storey['geometry']['faces']:
            if face['id'] == space['face_id']:
                break
            face = None
        if face is None:
            continue
        # print(f"Using face {face}")
        for edge in storey['geometry']['edges']:
            if edge['id'] in face['edge_ids']:
                for vid in edge['vertex_ids']:
                    # print(f"   {get_vertex_coords(storey, vid)}")
                    coords.add(get_vertex_coords(storey, vid))
    return coords

def embed_coords(g, mapping_file, floorplan_file):
    mapping = json.load(open(mapping_file))
    floorplan_data = json.load(open(floorplan_file))
    all_coords = {}
    for room_uri, space_name in mapping.items():
        min_y = float('inf')
        min_x = float('inf')
        max_y = float('-inf')
        max_x = float('-inf')
        coords = get_coords(floorplan_data, space_name)
        if len(coords) == 0:
            continue
        for x, y in coords:
            min_y = min(min_y, y)
            min_x = min(min_x, x)
            max_y = max(max_y, y)
            max_x = max(max_x, x)
        print(room_uri, [(min_x, min_y), (max_x, max_y)])
        all_coords[room_uri] = [(min_x, min_y), (max_x, max_y)]  # get_coords(space_name)

    adj_y = -min_y if min_y < 0 else min_y
    adj_x = -min_x if min_x < 0 else min_x


    # remove old coordinates
    g.g.remove((None, BRICK.hasCoordinates, None))

    for room_uri, coords in all_coords.items():
        # move to origin
        coords = [(x+adj_x, y+adj_y) for (x, y) in coords]
        # resizes
        coords = [(x/10, y/10) for (x, y) in coords]
        coords = list(coords)
        serialized_coords = json.dumps(coords)
        g.add((rdflib.URIRef(room_uri), BRICK.hasCoordinates,
               rdflib.Literal(serialized_coords)))
