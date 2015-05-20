#!/usr/bin/python
from config import *
import shutil
from jinja2 import Template


config_dict = {}
config_file_handle = open("./configuration.ini")



for line in config_file_handle:
    line_array = line.split("=")
    if len(line_array) > 1:
        key = line_array[0].strip().lower()
        val = line_array[1].strip()
        config_dict[key] = val



if (MODE == MODE):
	shutil.copy2("./data/schema_powertrack.json", "./image/schema.json");
	container_file = open("./image/container.yaml", 'w')
elif (MODE == "TWITTER"):
	shutil.copy2("./data/schema.json", "./image/schema.json");
	container_file = open("./image/container.yaml", 'w')
else:
	print "Invalid Mode. Mode can be GNIP or TWITTER only";


template_url = Template('URL = {{url}}')
template_proj_id = Template('PROJECT_ID = {{project_id}}')
template_proj_num = Template('PROJECT_NUMBER = {{project_number}}')
template_dataset_id = Template('DATASET_ID = {{dataset_id}}')
template_table_id = Template('TABLE_ID = {{table_id}}')
template_svc_accnt = Template('SERVICE_ACCOUNT = {{service_account}}')
template_key_file = Template('KEY_FILE = {{key_file}}')
template_cons_key = Template('CONSUMER_KEY = {{consumer_key}}')
template_cons_sct = Template ('CONSUMER_SECRET = {{consumer_secret}}')
template_acs_tok = Template('ACCESS_TOKEN = {{access_token}}')
template_acs_tok_sec = Template('ACCESS_TOKEN_SECRET = {{access_token_secret}}')
template_gnip_url = Template('GNIP_URL = {{gnip_url}}')
template_gnip_uname = Template('GNIP_USERNAME = {{gnip_username}}')
template_gnip_passwd = Template('GNIP_PASSWORD = {{gnip_password}}')

#Create a configuration file in the image directory
config_file_clean =  open("./image/config.py", 'w')
config_file_clean.write(template_url.render(url=config_dict['url'])+"\n")
config_file_clean.write(template_proj_id.render(project_id=config_dict['project_id'])+"\n")
config_file_clean.write(template_proj_num.render(project_number=config_dict['project_number'])+"\n")
config_file_clean.write(template_dataset_id.render(dataset_id=config_dict['dataset_id'])+"\n")
config_file_clean.write(template_table_id.render(table_id=config_dict['table_id'])+"\n")
config_file_clean.write(template_svc_accnt.render(service_account=config_dict['service_account'])+"\n")
config_file_clean.write(template_key_file.render(key_file=config_dict['key_file'])+"\n")
config_file_clean.write("with open(KEY_FILE, 'rb') as f:"+"\n\t"+"KEY = f.read()"+"\n")
config_file_clean.write(template_cons_key.render(consumer_key=config_dict['consumer_key'])+"\n")
config_file_clean.write(template_cons_sct.render(consumer_secret=config_dict['consumer_secret'])+"\n")
config_file_clean.write(template_gnip_url.render(gnip_url=config_dict['gnip_url'])+"\n")
config_file_clean.write(template_gnip_uname.render(gnip_username=config_dict['gnip_username'])+"\n")
config_file_clean.write(template_gnip_passwd.render(gnip_password=config_dict['gnip_password'])+"\n")

GNIP_SCHEMA_FILE = "./schema.json"
SCHEMA_FILE = "./schema.json"
config_file_clean.write("GNIP_SCHEMA_FILE = "+ "\"./schema.json\""+"\n")
config_file_clean.write("SCHEMA_FILE = "+ "\"./schema.json\""+"\n")


container_template_str  = "version: v1beta2" +"\n"+ "containers:"+"\n"+"\t"+"image: gcr.io/'https://www.googleapis.com/auth/bigquery'/{{image}}" +"\n\t"+"command: ['python' 'load.py']" +"\n\t"+"ports:"+"\n\t  "+"hostPort: 8080"+"\n\t  "+"containerPort: 8080";

container_template = Template(container_template_str)
container_template_str =  container_template.render(image=config_dict['image_type'])
container_file.write(container_template_str)
