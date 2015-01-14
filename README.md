Twitter for BigQuery
===

Requirements
---

sudo pip install pyopenssl --upgrade


Reading
---
https://cloud.google.com/bigquery/articles/dashboard#newappengineproject

https://cloud.google.com/appengine/docs/python/gettingstartedpython27/uploading
https://cloud.google.com/bigquery/authorization
http://stackoverflow.com/questions/20349189/unable-to-access-bigquery-from-local-app-engine-development-server

Sample Queries
---

- Geolocation
- Text search
- User search/mention
- Group by/comparison

Sample Data
---

The below files are provided in `/data` as samples of the data formats from Twitter and stored into BigQuery

/data

- schema.json - Tweet representation as BigQuery table schema
- sample_tweet.json - Tweet from stream 
- sample_tweet_cleaned.json - Tweet from stream, but scrubbed to be consistent with BigQuery/schema.json
- sample_stream.jsonr - Small sample of Twitter Stream, written to file

Credits
---

@tyler_treat - https://github.com/tylertreat/BigQuery-Python/
@apassant - https://github.com/apassant
@jay3dec (https://twitter.com/jay3dec) - http://code.tutsplus.com/tutorials/data-visualization-app-using-gae-python-d3js-and-google-bigquery--cms-22175

