import os
from os import walk
import thread
import gzip
from multiprocessing import Pool, Process, Queue

table = "gnip.ws2014"
mypath = "data"

def process_file(file_gz, output):

	file = file_gz[:-3]
	
	call_unzip = "gunzip %s" % (file_gz)
	print call_unzip
	os.system(call_unzip)
	
	call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
	print call_batch
	os.system(call_batch)
	
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
  		for f in filenames[:10]:
  			if f.endswith(".gz"):
  				file = "%s/%s" % (dirpath, f)
  				files.append(file)

	output = Queue()

 	processes = [Process(target=process_file, args=(f, output)) for f in files]

 	for p in processes:
 	    p.start()
   			
 	for p in processes:
 	    p.join()
	
	# Get process results from the output queue
 	results = [output.get() for p in processes]
	
 	print results

# 	# start 4 worker processes
# 	with Pool(processes=4) as pool:
# 
# 		result = pool.apply_async(process_file, files)
# 		print result.get(timeout=1)
		
#  			
#  			print f
#  			
# 
# 				p = Process(target=process_file, args=(dirpath, f,))
# 				p.start()
# 				p.join()
 				
		


		




