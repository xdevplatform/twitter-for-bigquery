FROM google/python

RUN apt-get install -y libffi-dev

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install bigquery-python
RUN pip install --upgrade google-api-python-client

ADD config.py /config.py
ADD utils.py /utils.py
ADD load_gnip.py /load_gnip.py
ADD key.p12 /key.p12
ADD logging.conf /logging.conf
ADD schema_powertrack.json /schema_powertrack.json

CMD python load_gnip.py
