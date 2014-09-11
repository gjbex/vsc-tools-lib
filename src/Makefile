DB_DIR = ~/tmp/
DB_NAME = nodes.db

DB = $(DB_DIR)$(DB_NAME)

all: create load

create:
	./create_node_db.py --db $(DB)

load:
	./load_node_db.py --db $(DB) --file ../data/pbsnodes.txt

clean:
	rm -f $(DB)

