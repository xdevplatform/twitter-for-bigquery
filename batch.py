import os
from os import walk
import thread
import gzip
from multiprocessing import Pool, Process, Queue

table = "gnip.ws2014"
mypath = "data"

def process_file(file_gz, output=None):

	file = file_gz[:-3]
	
	call_unzip = "gunzip %s" % (file_gz)
	print call_unzip
	os.system(call_unzip)
	
	call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
	print call_batch
	os.system(call_batch)
	
	if output:
		output.put("%s: Done" % (file))

def info(title):
    print(title)
    print('module name:', __name__)
    if hasattr(os, 'getppid'):  # only available on Unix
        print('parent process:', os.getppid())
    print('process id:', os.getpid())

def f(name):
    info('function f')
    print('hello', name)

if __name__ == '__main__':
	
# 	info('main line')
# 	p = Process(target=f, args=('bob',))
# 	p.start()
# 	p.join()
#     
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


 				
		


		




