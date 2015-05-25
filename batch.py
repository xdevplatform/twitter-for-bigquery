import os
from os import walk
import gzip

table = "gnip.ws2014"
mypath = "data"

f = []
for (dirpath, dirnames, filenames) in walk(mypath):
	for f in filenames:
		
		if f.endswith(".gz"):
		
			file_gz = "%s/%s" % (dirpath, f)
			file = file_gz[:-3]
			
# 			print file
			
 			call_unzip = "gunzip %s" % (file_gz)
 			print call_unzip
 			os.system(call_unzip)
 			
 			call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
 			print call_batch
 			os.system(call_batch)
		
#  			exit()

		




