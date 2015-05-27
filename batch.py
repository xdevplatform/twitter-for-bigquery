import os
from os import walk
import sys
import thread
import gzip
from multiprocessing import Pool, Process, Queue

PROCESS_COUNT = 8

# file utilities
class Utils:

    @staticmethod
    def rename(file, to):

        call_rename = "mv %s %s" % (file, to)
        print call_rename
        os.system(call_rename)

        return to

    @staticmethod
    def archive(file):

        if ".archive" in file:
            return file 

        file_archive = "%s.archive" % file
        Utils.rename(file, file_archive)

        return file_archive

    @staticmethod
    def unarchive(file):

        if ".archive" not in file:
            return file 

        file2 = file[:-8]
        Utils.rename(file, file2)

        return file2

    @staticmethod
    def gzip(file):

        if ".gz" in file:
            return file 

        call_zip = "gzip %s" % (file)
        print call_zip
        os.system(call_zip)

        return "%s.gz" % file

    @staticmethod
    def gunzip(file):

        if ".gz" not in file:
            return file 

        call_unzip = "gunzip %s" % (file)
        print call_unzip
        os.system(call_unzip)

        return file[:-3]

    @staticmethod
    def cat_all(path, file):

        call_cat = "find %s -type f -exec cat {} + > %s" % (path, file)
        print call_cat
        os.system(call_cat)

    @staticmethod
    def get_files(path):

        files = []

        # process all files and let async handle archiving 
        for (dirpath, dirnames, filenames) in walk(path):
            for f in filenames:
                file = "%s/%s" % (dirpath, f)
                files.append(file)

        return files

# restore file to original gzip 
def reset_file(file, table, output=None):

    if ".archive" in file:
        file = Utils.unarchive(file)

    # ignore archive files
    if file.endswith(".json"):
        file = Utils.gzip(file)

    if output:
        output.put(file)

    return file

def process_files(path, table):

    files = Utils.get_files(path)

    # if zipped, unzip for loading
    for f in files:
        if "json.gz" in f:
            file = Utils.gunzip(f)

    # get new list of files
    files = Utils.get_files(path)

    file_result = "master.json"
    Utils.cat_all(path, file_result)

    file_gz = Utils.gzip(file_result)

    # load to bigquery
    call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=500000 %s %s" % (table, file_result)
    print call_batch
    os.system(call_batch)

    return file_result

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print "Usage: batch.py [reset|process] [file|directory] <table>"

    (script, action, path, table) = sys.argv

    files = []
    processes = []


    function = None
    if action == 'reset':

        if os.path.isfile(path):
            files.append(path)
        else:
            files = Utils.get_files(path)

        pool = Pool(processes=PROCESS_COUNT)
        results = [pool.apply_async(reset_file, args=(f, table)) for f in files]

        output = [p.get() for p in results]
        print output

    elif action == 'process':

        process_files(path, table)


