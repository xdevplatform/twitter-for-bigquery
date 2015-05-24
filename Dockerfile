FROM google/python

RUN apt-get install -y libffi-dev

RUN pip install --upgrade pip
RUN pip install config
RUN pip install tweepy
RUN pip install bigquery-python
RUN pip install --upgrade google-api-python-client

ADD config /config
ADD schema /schema
ADD key.p12 /key.p12
ADD logging.conf /logging.conf
ADD libs /libs
ADD utils.py /utils.py
ADD load.py /load.py

CMD python load.py
