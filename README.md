Twitter for BigQuery
===

Requirements
---


Reading
---
https://cloud.google.com/bigquery/articles/dashboard#newappengineproject
https://cloud.google.com/appengine/docs/python/gettingstartedpython27/uploading
https://cloud.google.com/bigquery/authorization

Extensions

- YouTube
- Sentiment Analysis
- Cross reference other sources

Sample Queries
---

https://cloud.google.com/bigquery/query-reference

- Tweet source

    SELECT source, count(*) as count FROM [tweets.2015_01_09] GROUP by source ORDER BY count DESC LIMIT 1000
    
- Text search

	SELECT text FROM [tweets.2015_01_09] WHERE text CONTAINS ' something ' LIMIT 10

- Geolocation

	SELECT text FROM [tweets.2015_01_09] WHERE coordinates.coordinates.lat IS NOT NULL LIMIT 10

- Media/URLs shared

	SELECT entities.media.type as media_type, count(*) as count FROM [tweets.2015_01_09] GROUP BY media_type ORDER BY count LIMIT 10
	SELECT text, entities.urls.url FROM [tweets.2015_01_09] WHERE entities.urls.url IS NOT NULL LIMIT 10

- User activity

	SELECT user.screen_name, count(*) as count FROM [tweets.2015_01_09] GROUP BY user.screen_name ORDER BY count DESC LIMIT 10


Sample Data
---

The below files are provided in `/data` as samples of the data formats from Twitter and stored into BigQuery

/data

- sample_stream.jsonr - Small sample of Twitter Stream, written to file
- sample_tweet_cleaned.json - Tweet from stream, but scrubbed to be consistent with BigQuery/schema.json
- sample_tweet.json - Tweet from stream 
- schema.json - Tweet representation as BigQuery table schema


Credits
---

@tyler_treat - https://github.com/tylertreat/BigQuery-Python/
@apassant - https://github.com/apassant
@jay3dec (https://twitter.com/jay3dec) - http://code.tutsplus.com/tutorials/data-visualization-app-using-gae-python-d3js-and-google-bigquery--cms-22175

