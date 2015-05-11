# -*- coding: utf-8 -*-

import threading

import requests

from gnippy import config

class PowerTrackClient():
    """
        PowerTrackClient allows you to connect to the GNIP
        power track stream and fetch data.
    """
    callback = None
    url = None
    auth = None

    def __init__(self, callback, **kwargs):
        self.callback = callback
        c = config.resolve(kwargs)
        self.url = c['url']
        self.auth = c['auth']

    def connect(self):
        self.worker = Worker(self.url, self.auth, self.callback)
        self.worker.setDaemon(True)
        self.worker.start()

    def disconnect(self):
        self.worker.stop()
        self.worker.join()

    def load_config_from_file(self, url, auth, config_file_path):
        """ Attempt to load the config from a file. """
        conf = config.get_config(config_file_path=config_file_path)

        if url is None:
            conf_url = conf['PowerTrack']['url']
            if conf_url:
                self.url = conf['PowerTrack']['url']
        else:
            self.url = url

        if auth is None:
            conf_uname = conf['Credentials']['username']
            conf_pwd = conf['Credentials']['password']
            self.auth = (conf_uname, conf_pwd)
        else:
            self.auth = auth


class Worker(threading.Thread):
    """ Background worker to fetch data without blocking """
    def __init__(self, url, auth, callback):
        super(Worker, self).__init__()
        self.url = url
        self.auth = auth
        self.on_data = callback
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        r = requests.get(self.url, auth=self.auth, stream=True)
        for line in r.iter_lines():
            if self.stopped():
                break
            elif line:
                self.on_data(line)