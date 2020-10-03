# Fault Dashboard

1. Install dependencies in `requirements.txt`
2. Download the `bricksql` binary from https://github.com/gtfierro/reasonable/releases/tag/conix-v1 and the `Brick.n3` file and put them in the same directory.
3. Run `bricksql`: `./bricksql`
4. Initialize the databases using a config file: `python initialize.py configs/ciee.yml`
5. Start the fault dashboard `python app.py configs/ciee.yml`
6. Visit [http://localhost:8081](http://localhost:8081)
