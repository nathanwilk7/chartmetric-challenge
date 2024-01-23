# chartmetric-challenge

## 1. Database Schema Diagram

https://whimsical.com/media-playlist-er-FR744ur8Rzc9zCHSQE9L5X@2Ux7TurymNGL7mTyTqqS
(you should be able to view this board with the link above)

## 2/3. JSON->CSV and CSV->PG

Open the drive link: https://drive.google.com/drive/folders/14d0y4uR4s7om6QXAZvaFelsbYbpv4zE0

Download the `youtube_playlist.json` file and store it in the current directory if it's not already there.

Requirements
- Python 3.10 (ish)
- Docker
- Bash (*nix, no Windows path support yet)

Setup/activate your python env:

```sh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```sh
python -m pytest
```

Start a local postgres (Note, hardcoded PG to run on port 5454. Make sure you run this cmd from the root of this repo b/c it uses pwd):

```sh
docker run -p 5454:5432 -e POSTGRES_PASSWORD=example -e POSTGRES_USER=example -e POSTGRES_DB=chartmetric_challenge -v $(pwd)/postgres_init:/docker-entrypoint-initdb.d/ postgres
```
Run the script with:

```sh
python main.py youtube_playlist.json PLAYLIST_YOUTUBE outputdir postgresql://example:example@localhost:5454/chartmetric_challenge
```

The output is saved to `outputdir`.

## 4. Match ISRC's

Download the `cm_track.csv` file from the drive link above and put it in the current directory.

Insert the ISRC data into the postgres database by running this Python script:

```sh
python isrc.py
```

Connect to the postgres database (when prompted for a password, enter `example`):

```sh
psql -h localhost -p 5454 -U example chartmetric_challenge
```

Then, here is a slow example query to match ISRC's. I didn't spend the time to look
too deep into this, but if I spent more time I would speed it up and try out some regexes, 
removing punctuation, etc to find more matches.

```sql
select
  i.artist, a.name, i.track, miml.secondary_title, miml.primary_title
from media_items mi
join media_item_metadata_log miml on miml.media_item_id = mi.id
join isrc_lookup i on (
    i.track ilike miml.secondary_title
    or i.track ilike miml.primary_title
)
join artists a on a.name ilike i.artist;
```

## 5. Handle Bad/Missing Data

There are a couple common types of bad/missing data, let's discuss some of the common ones.

1. Missing fields. We handle non-required fields by allowing them to be null in the database. If a required field was null, I would raise an alert/error and put it onto a dead letter queue.
2. Malformed data. If we got malformed data (eg bad json), I would raise an alert/error and put it onto a dead letter queue.
3. Bugs. If we have bad/missing data due to bugs in our code, then I would fix the bugs and rerun the ingest.

Something that is key is to make your ingest pipelines idempotent such that you can rerun
them multiple times without duplicating the downstream data.

## 6. Scale Pipline Up/Out

This code I've written here is very much designed to run on a single node, so you could
scale it up by putting it on a bigger machine. Duckdb should utilize the extra memory/cpu
well and you could add some more smarts to the Python code to have it parallelize IO as well.
However, if I were to build this pipeline scalably for my job I would likely take one of the
below approaches:

1. Partition the input so that ingest becomes embarrassingly parallel. For example, if your
ingest API allows you to filter by time-range or playlist-prefix, then instead of running one
big job, you can have N smaller jobs. Each of these jobs, is essentially just going to loop
through the input from API and do an upsert into the DB for each row. Keep in mind that
with this eventually you will bottleneck on your DB unless you're careful.

2. Take an event sourcing/async approach. Instead of treating ingest as a synchronous job,
have your ingest workers trigger events for the data which needs to be ingested. Then,
your downstream stream processers can transform that data into an appropriate format
or database. The nice of doing this async is that you're introducting a natural buffer
for your database which will give you longer until it bottlenecks. In addition, this
approach lends itself really well to saving a bunch timeseries data in a log, so there
is less of an impedence mismatch between the code you write and your end goal. One challenge
with this approach is that async evented systems require lots of thought for how to ensure
a positive user experience (eg eventually consistency issues). Another challenge is that
while streaming systems are mature, they are not as simple operationally as synchronous
databases.

## 7. Database Choice + Dashboard

I think it's important to keep in mind that NoSQL is not a very useful term, sort of like
saying I need a non-car transportation vehicle to get somewhere, could be you need a bike or
an airplane or a boat. Most people use the work NoSQL to describe non-relational distributed databases like Mongo and Cassandra. The problems people are usually trying to solvrese when
exploring NoSQL options are often related to flexibility and horizontal scaling. It's a
significant investment to scale out traditional SQL databases whereas Mongo and Cassandra
are much easier to scale out operationally. A last caveat here is that I am strong believer
in analyzing read/write patterns and prototyping/benchmarking when choosing a database.
If I were choosing a replacement for Aurora at Chartmetric, then the first thing I would do
is find/add some metrics around the most important read/write queries and then start trying
out load tests on some different database options in a "staging" environment with 
fairly realistic data shapes and scale. But I'll try to answer the spirit of this question
with only the information I have now.

Presumably, there are three main read/write patterns at Chartmetric:
1. Large batch write jobs. These probably run daily and/or hourly and likely SLAM the database while running.
2. Dashboards to show the current top songs. These reads are going to be using the latest available data and will have some queries which show a global overview and others with filters like a specific artist/genre/etc.
3. Dashboards to show the history of specific artists/genres/etc. These reads are going to hit a ton of timeseries data and most users will expect that the more data they load, the longer they'll have to wait.

Here are some of the databases I would explore:
1. Per the boring technology club, you should always first try to push your current system to its limit before exploring other options. For example, if ingest load is your biggest problem now, then you could introduce a buffer queue for writes to avoid spikes. Another thing you could try is background jobs to compact data as it ages. For example, after 90 days only store weeky snapshots of data and after 365 days only store monthly snapshots, this would give you 1-2 orders of magnitude less time-series data and if you pick the right cutoffs, users won't mind.
2. If your main problems are the timeseries data, then using the Timescale extension for Postgres is the obvious choice (not sure if Aurora lets you use this and you'd want to think about the license options). There are some details you'd have to figure out, but Timescale is designed for large IoT datasets and the timeseries data at Chartmetric should be a good fit.
3. Partition Postgres by day/week/month/etc. You're probably already doing this, but if not this would be a way to handle timeseries issues but wouldn't solve ingest without partitioning on non-time attributes as well.
4. Move "cold" data into OLAP storage. Keep only the last 90 days of data in Postgres and continually move the older data into Iceberg or whatever. Queries which want current/date ranges within 90 days use Postgres while those which include older ranges hit Iceberg (via Trino, DuckDB, Clickhouse, etc). The tricky part of this is figuring out how to manage when timeranges cross the cold boundary, but you could probably come up with something and this approach would greatly reduce cost.
5. You could double-write your data into Postgres and a time-series database like Influx/QuestDB and then engineers can decide which ones to query based on their use case.
6. You could use a distributed key-value store like Cassandra or Mongo and then index your slowest reads. Most key-value storage systems like this will either store lots of duplicated data/have slow updates or you'll have to do joins client-side, but the nice part is that you can scale them out more easily as long as your read/write patterns have natural partitions that within their data models. I expect these databases would handle the write volume well.
7. There are some "NewSQL" databases (eg Spanner, CockroachDB, YugabyteDB) which are probably still too expensive to be a good fit and don't necessarily give you a ton of scalability, but have the nice property that you don't have to migrate off the relational model and can sometimes get more scale than a single writer node.

We can go deeper into these at a future date, but hopefully that gives a taste of how I'd think about this.

Note, I've left a bunch of `NOTE` comments throughout the code I would address if I had more time.