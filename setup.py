#!/usr/bin/python
import shutil
from config import Config
from jinja2 import Template
from utils import Utils

f = file("./config")
config = Config(f)

schema = None
if (config.MODE not in ["gnip", "twitter"]):
	print "Invalid Mode. Mode can be 'gnip' or 'twitter' only";
	exit()

container_template = Utils.read_file("./container.yaml.template")
container_template = Template(container_template).render(config)

container_file = open("./container.yaml", 'w')
container_file.write(container_template)


