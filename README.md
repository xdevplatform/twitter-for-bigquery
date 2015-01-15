Twitter for BigQuery
===

Requirements
---


Relevant Reading
---

- *Authorizing Access to the Google BigQuery API using OAuth 2.0:* https://cloud.google.com/bigquery/authorization
- *Codelab: Creating a BigQuery Dashboard:* https://cloud.google.com/bigquery/articles/dashboard#newappengineproject
- *Uploading Your Application:* https://cloud.google.com/appengine/docs/python/gettingstartedpython27/uploading
- *Data Visualization App Using GAE Python, D3.js and Google BigQuery:* http://code.tutsplus.com/tutorials/data-visualization-app-using-gae-python-d3js-and-google-bigquery--cms-22175
- *How to stream data from Twitter with tweepy [Python]:* http://runnable.com/Us9rrMiTWf9bAAW3/how-to-stream-data-from-twitter-with-tweepy-for-python

Setting up with sample data source
---

Sample Queries
---

Counts (Bar/Donut/Pie)
Time Sequence (Line)
Location (Map)

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


Loading more recent data
---

The sample Twitter data set is useful to get you exploring with Twitter data. If you want to explore with more data, you can use the 
included pipeline code to populate data directly from the Twitter stream.



Other projects/ideas
---

Twitter data becomes even more interesting when combined with other sources of information or analysis. Here are some ideas
that people have done that might inspire your next project 

- Cross referencing Twitter data with other sources to show vertical/topical trending data (inspired by http://apassant.net/2014/11/25/24-discover-youtube-music-trends-twitter/)
- Sentiment analysis on twwet

Sample JSON files
---

The below JSON files are provided in `/data` as samples of the data formats from Twitter and stored into BigQuery.

/data

- sample_stream.jsonr - Small sample of Twitter Stream, written to file
- sample_tweet_cleaned.json - Tweet from stream, but scrubbed to be consistent with BigQuery/schema.json
- sample_tweet.json - Tweet from stream 
- schema.json - Tweet representation as BigQuery table schema


Credits
---

The following developers and bloggers have aided greatly in the development of this source. I'm deeply appreciative
of their open source code and knowledge sharing to make this possible!

@tyler_treat - https://github.com/tylertreat/BigQuery-Python/
@apassant - https://github.com/apassant
@jay3dec (https://twitter.com/jay3dec) - http://code.tutsplus.com/tutorials/data-visualization-app-using-gae-python-d3js-and-google-bigquery--cms-22175

