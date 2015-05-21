#!/usr/bin/python
from jinja2 import Template
import shutil

import config
from utils import Utils

schema = None
if (config.MODE == "gnip"):
	schema = "./data/schema_powertrack.json"
elif (config.MODE == "twitter"):
	schema = "./data/schema.json"
else:
	print "Invalid Mode. Mode can be 'gnip' or 'twitter' only";

shutil.copy2(schema, "./schema.json");
shutil.copy2(schema, "./image/schema.json");
shutil.copy2("key.p12", "./image/key.p12");

props = {}
for name in (name for name in dir(config) if not name.startswith('_')):
    props[name] = getattr(config, name, '')
            
container_template = Utils.read_file("./container.yaml.template")
container_template = Template(container_template).render(props)

container_file = open("./image/container.yaml", 'w')
container_file.write(container_template)

KEY = None
with open (config.KEY_FILE, "rb") as f:
    KEY = f.read()

