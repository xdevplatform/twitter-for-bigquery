import time

def scrub(d):

    # d.iteritems isn't used as you can't del or the iterator breaks.
    for key, value in d.items():
        
        if value is None:
            del d[key]
        elif key == 'coordinates':
            del d[key]
        elif key == 'retweeted_status':
            del d[key]
        elif key == 'created_at':
            d[key] = convert_timestamp(value)
        elif isinstance(value, dict):
            scrub(value)
    return d  # For convenience

def all_keys(d):

    arr = []
    
    for key, value in d.items():
        
        if isinstance(value, dict):
            k = all_keys(value)
            arr = arr + k
        else:
            arr.append(key)
            
    return arr

def convert_timestamp(str):
    
    ts = time.strptime(str,'%a %b %d %H:%M:%S +0000 %Y')
    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
    
    return ts

def read_file(fn):
    
    data = ""
    with open(fn, "r") as f:
        for line in f:
            data = data + line
            
    return data    