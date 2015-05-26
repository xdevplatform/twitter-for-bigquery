import os
from os import walk
import thread
import gzip
from multiprocessing import Pool, Process, Queue

table = "gnip.ws2014"
mypath = "data"

def process_file(file_gz, output=None):

	file = file_gz[:-3]
	
	# unpack file
	call_unzip = "gunzip %s" % (file_gz)
	print call_unzip
	os.system(call_unzip)
	
	# load to bigquery
	call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
	print call_batch
	os.system(call_batch)

	# pack file
	call_zip = "gzip %s" % (file)
	print call_zip
	os.system(call_zip)
	
	if output:
		output.put(file_gz)

	return file_gz

if __name__ == '__main__':
	
 	files = []
 	processes = []
 	
  	for (dirpath, dirnames, filenames) in walk(mypath):
  		for f in filenames:
  			if f.endswith(".gz"):
  				file = "%s/%s" % (dirpath, f)
  				files.append(file)

	pool = Pool(processes=10)
	results = [pool.apply_async(process_file, args=(f,)) for f in files]
	
	output = [p.get() for p in results]
	print output


 				
		


		




