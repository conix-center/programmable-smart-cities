# Year 3 Demo

## Setting Up A Building

This configures a local repository of building data + metadata to be used in the demo.

In the `buildings/` directory, place the following files:

1. Turtle files: all files ending with `.ttl` will be loaded into a Brick graph representing the building. We recomment including a copy of the [latest Brick.ttl](https://github.com/BrickSchema/Brick/releases/download/nightly/Brick.ttl)
2. Data files: a set of CSV files in a `data/` directory, each with the schema `time,id,value` where `id` is the name of the Brick entity

    Example:
    
    ```
    time,id,value
    2018-01-01T00:01:14Z,http://xbos.io/ontologies/ciee#hamilton_0022_lux,51.22030245859585
    2018-01-01T00:01:25Z,http://xbos.io/ontologies/ciee#hamilton_0022_lux,50.93317300259406
    2018-01-01T00:06:48Z,http://xbos.io/ontologies/ciee#hamilton_0022_lux,43.706769339198765
    ```
    
3. `floorplan.json`: this is a JSON file exported from [the NREL floorplan maker](https://nrel.github.io/floorspace.js/). You can upload an image to the site and create "Spaces" by clicking to create vertices. Remember to create a new Space for each room, and *at this point in time* only create regular quadrilaterals; arbitrary polygons do not work.

4. `mapping.json`: a single JSON dictionary of `Brick entity => Space ID`; this encodes which Spaces in the floorplan file correspond to which Brick rooms/floors/zones in the `.ttl` files

    See `buildings/ciee` for an example (the `data.tar.gz` file produces the `data/` folder):

    ```
    $ tree buildings/ciee/
    buildings/ciee/
    ├── Brick.ttl
    ├── ciee.ttl
    ├── data
    │   ├── 2018-01-01T00:00:00Z.csv
    │   ├── 2018-01-02T00:00:00Z.csv
    │   ├── 2018-01-03T00:00:00Z.csv
    │   ├── 2018-01-04T00:00:00Z.csv
    │   ├── 2018-01-05T00:00:00Z.csv
    │   ├── 2018-01-06T00:00:00Z.csv
    │   ├── 2018-01-07T00:00:00Z.csv
    │   ├── 2018-01-08T00:00:00Z.csv
    │   └── 2018-01-09T00:00:00Z.csv
    ├── data.tar.gz
    ├── floorplan.json
    └── mapping.json
    ```

## Running the VR Demo

Currently, the demo renders a rough floor plan and streams historical temperature data to color the different rooms on the floor plan

1. Install dependencies in `client/requirements.txt` (if you don't want
   `docker` as a dependency, replace `brickschema[allegro]` with `brickschema`.
   If you are on a recent Linux system and want to try some cool new reasoning
   stuff that's much faster, replace the line with `brickschema[reasonable]`)
2. Prepare a building folder (after unzipping `ciee/data.tar.gz`,
   `buildings/ciee` should be ready to go)
3. Execute the client: `python client/main.py buildings/ciee ciee.db ciee`:
    - arg 1: path to the folder containing the necessary files
    - arg 2: name of a sqlite3 database to store timeseries, metadata (call
      this `:memory:` if you don't want to persist -- this DB file is
      essentially a cache to avoid expensive recomputation)
    - arg 3: the name of the ARENA scene
4. **note**: the ARENA objects are streamed and not persisted, so you will need
   to navigate to the scene in your browser *before* starting the client.


## Available Buildings
1. `buildings/ciee`: The associated files are contained in this repository.
2. `buildings/ebu3b`: Its Tarball is available [here](https://drive.google.com/file/d/1MHihVVQevC7kncBmbu4IAuyYKvIozRE3/view?usp=sharing). You should be a member of CONIX to access the file.
