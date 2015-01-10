def scrub(record):
    return del_none(record)
 
def del_none(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # d.iteritems isn't used as you can't del or the iterator breaks.
    for key, value in d.items():
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d  # For convenience

def convert_timestamp(str):
    
    ts = time.strptime(str,'%a %b %d %H:%M:%S +0000 %Y')
    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
    
    return ts