#!/usr/bin/python
import shutil
from config import Config
from jinja2 import Template
from utils import Utils

f = file("./config")
config = Config(f)

schema = None
if (config.MODE == "gnip"):
	schema = "./data/schema_powertrack.json"
elif (config.MODE == "twitter"):
	schema = "./data/schema.json"
else:
	print "Invalid Mode. Mode can be 'gnip' or 'twitter' only";

# files for app
shutil.copy2(schema, "./schema.json");

# files for image
shutil.copy2("./config", "./image/config");
shutil.copy2("key.p12", "./image/key.p12");
shutil.copy2(schema, "./image/schema.json");

container_template = Utils.read_file("./container.yaml.template")
container_template = Template(container_template).render(config)

container_file = open("./image/container.yaml", 'w')
container_file.write(container_template)


