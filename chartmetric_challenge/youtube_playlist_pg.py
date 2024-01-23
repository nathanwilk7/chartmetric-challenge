from dateutil import parser
import datetime
import logging
import os

import duckdb
import pandas as pd

from chartmetric_challenge.constants import (
    ORG_YOUTUBE,
    ARTISTS_FILENAME,
    MEDIA_ITEMS_FILENAME,
    USERS_FILENAME,
    MEDIA_ITEM_METADATA_FILENAME,
    PLAYLIST_METADATA_LOG_FILENAME,
    PLAYLIST_PLAYS_LOG_FILENAME,
    PLAYLIST_POSITIONS_LOG_FILENAME,
    PLAYLISTS_FILENAME,
)


# NOTE could separate convertion from ingestion
# NOTE DRY up the below code
# NOTE could add an ABC "interface" for ingestors or similar
class YoutubePlaylistPgIngestor:
    '''
    Given a JSON file of YouTube playlist data, convert it to CSVs and ingest
    them into the specified Postgres database.

    Currently only supports day granularity.
    '''
    def __init__(self, granularity: str, source_path: str, output_directory: str, pg_connection_string: str):
        self._source_path = source_path
        self._output_directory = output_directory
        self._source_name = ORG_YOUTUBE
        self._playlists_output_path = os.path.join(self._output_directory, PLAYLISTS_FILENAME)
        self._users_output_path = os.path.join(self._output_directory, USERS_FILENAME)
        self._media_items_output_path = os.path.join(self._output_directory, MEDIA_ITEMS_FILENAME)
        self._artists_output_path = os.path.join(self._output_directory, ARTISTS_FILENAME)
        self._media_item_metadata_log_output_path = os.path.join(self._output_directory, MEDIA_ITEM_METADATA_FILENAME)
        self._playlist_metadata_log_output_path = os.path.join(self._output_directory, PLAYLIST_METADATA_LOG_FILENAME)
        self._playlist_plays_log_output_path = os.path.join(self._output_directory, PLAYLIST_PLAYS_LOG_FILENAME)
        self._playlist_positions_log_output_path = os.path.join(self._output_directory, PLAYLIST_POSITIONS_LOG_FILENAME)
        self._pg_connection_string = pg_connection_string
        self._granularity = granularity

    def ingest(self):
        logging.info(f"Starting load")
        df = self.load()
        logging.info(f"Starting conversion of {len(df)} rows")
        self.convert(df)
        logging.info(f"Starting write to postgres")
        self.csvs_to_pg()

    def load(self) -> pd.DataFrame:
        if not os.path.exists(self._source_path):
            raise ValueError(f"source_path {self._source_path} does not exist")

        return pd.read_json(self._source_path, orient='records')


    def convert(self, df: pd.DataFrame):
        try:
            os.makedirs(self._output_directory, exist_ok=True)
        except OSError:
            raise ValueError(f"output_directory {self._output_directory} is not a valid path")

        duckdb.register('df', df)

        self._create_and_register_ids('df', 'playlist_id', True, self._playlists_output_path, 'playlists')
        self._create_and_register_ids('df', 'channel_id', True, self._users_output_path, 'users')
        self._create_and_register_ids('df', 'video_id', True, self._media_items_output_path, 'media_items')
        # NOTE handle artist name casing issues, misspellings, etc
        self._create_and_register_ids('df', 'artist_name', False, self._artists_output_path, 'artists', output_id_col_name='name')

        # NOTE I thought about doing all these transformations in Python, but
        # then found it wasn't too much work to do in SQL. I would likely
        # reconsider if I had to do more complex transformations.
        enriched_df = duckdb.query(
'''
-- Replaces the source-specific ID's with the ID's we generated
SELECT
    playlists.id AS playlist_id,
    df.playlist_name,
    df.artwork_url AS cover_url,
    users.id AS user_id,
    df.views AS plays,
    df.num_videos AS num_media_items,
    df.timestp,
    media_items.id AS media_item_id,
    df.title AS primary_title,
    artists.id AS artist_id,
    df.image_url AS media_cover_url,
    df.track_title AS secondary_title,
    df.position,
FROM
    df
    JOIN playlists ON df.playlist_id = playlists.source_id
    LEFT JOIN users ON df.channel_id = users.source_id
    JOIN media_items ON df.video_id = media_items.source_id
    LEFT JOIN artists ON df.artist_name = artists.name
'''
        )
        duckdb.register('enriched_df', enriched_df)

        minimum_timestp = duckdb.query(
'''
-- Find the first timestamp in the data to start loading from
SELECT MIN(timestp) FROM enriched_df
''').fetchone()[0]
        maximum_timestp = duckdb.query(
'''
-- Find the last timestamp in the data to stop loading at
SELECT MAX(timestp) FROM enriched_df
''').fetchone()[0]
        # NOTE haven't added support for other granularities yet
        if self._granularity == 'day':
            all_intervals = []
            cur_start_day = parser.parse(minimum_timestp).date()
            last_day = parser.parse(maximum_timestp).date()
            while cur_start_day <= last_day:
                all_intervals.append(cur_start_day.isoformat())
                cur_start_day += datetime.timedelta(days=1)
        else:
            raise ValueError(f"granularity {self._granularity} is not supported")

        strptime_format = '%Y-%m-%d'
        self._playlist_metadata_log_csv(all_intervals, strptime_format)
        self._playlist_plays_log_csv(all_intervals, strptime_format)
        self._playlist_positions_log_csv(all_intervals, strptime_format)
        # NOTE Turns out this is 1:1 with media_items in this dataset, 
        # I'm not going to redo it but it would make queries simpler if I did
        self._media_item_metadata_log_csv(all_intervals, strptime_format)

    def _create_and_register_ids(
        self,
        input_df_name: str,
        id_col_name: str,
        include_source: bool,
        output_path: str,
        register_name: str,
        output_id_col_name: str = 'source_id',
    ):
        id_df = duckdb.query(
f'''
-- This assigns a unique int ID to each unique value in the specified ID column
WITH temp_table AS (
    SELECT DISTINCT {id_col_name} FROM {input_df_name}
)
SELECT
    row_number() OVER () AS id,
    {"'" + self._source_name + "' AS source," if include_source else ''}
    {id_col_name} AS {output_id_col_name},
FROM
    temp_table
WHERE
    {id_col_name} IS NOT NULL
''').to_df()
        id_df.to_csv(output_path, index=False)
        duckdb.register(register_name, id_df)

    def _playlist_metadata_log_csv(
        self,
        all_intervals: list[str],
        strptime_format: str,
    ):
        pml_table_name = 'playlist_metadata_log'
        pml_create_table_query = f'''
CREATE TABLE {pml_table_name} (
    playlist_id BIGINT,
    ingest_timestamp TIMESTAMP,
    playlist_name VARCHAR,
    cover_url VARCHAR,
    user_id BIGINT,
    num_media_items INT,
    PRIMARY KEY (playlist_id, ingest_timestamp)
);
'''
        duckdb.sql(f'DROP TABLE IF EXISTS {pml_table_name}')
        duckdb.sql(pml_create_table_query)
        self._create_log_df(
            pml_table_name,
            ['playlist_id'],
            ['playlist_name', 'cover_url', 'user_id', 'num_media_items'],
            all_intervals,
            self._granularity,
            strptime_format,
            self._playlist_metadata_log_output_path,
        )

    def _playlist_plays_log_csv(
        self,
        all_intervals: list[str],
        strptime_format: str,
    ):
        ppl_table_name = 'playlist_plays_log'
        ppl_create_table_query = f'''
CREATE TABLE {ppl_table_name} (
    playlist_id BIGINT,
    ingest_timestamp TIMESTAMP,
    plays bigint,
    PRIMARY KEY (playlist_id, ingest_timestamp)
);
'''
        duckdb.sql(f'DROP TABLE IF EXISTS {ppl_table_name}')
        duckdb.sql(ppl_create_table_query)
        self._create_log_df(
            ppl_table_name,
            ['playlist_id'],
            ['plays'],
            all_intervals,
            self._granularity,
            strptime_format,
            self._playlist_plays_log_output_path,
        )

    def _playlist_positions_log_csv(
        self,
        all_intervals: list[str],
        strptime_format: str,
    ):
        pposl_table_name = 'playlist_positions_log'
        pposl_create_table_query = f'''
CREATE TABLE {pposl_table_name} (
    playlist_id BIGINT,
    media_item_id BIGINT,
    ingest_timestamp TIMESTAMP,
    position int,
    PRIMARY KEY (playlist_id, media_item_id, ingest_timestamp)
);
'''
        duckdb.sql(f'DROP TABLE IF EXISTS {pposl_table_name}')
        duckdb.sql(pposl_create_table_query)
        self._create_log_df(
            pposl_table_name,
            ['playlist_id', 'media_item_id'],
            ['position'],
            all_intervals,
            self._granularity,
            strptime_format,
            self._playlist_positions_log_output_path,
        )

    def _media_item_metadata_log_csv(
        self,
        all_intervals: list[str],
        strptime_format: str,
    ):
        miml_table_name = 'media_item_metadata_log'
        miml_create_table_query = f'''
CREATE TABLE {miml_table_name} (
    media_item_id BIGINT,
    ingest_timestamp TIMESTAMP,
    primary_title VARCHAR,
    secondary_title VARCHAR,
    artist_id BIGINT,
    media_cover_url VARCHAR,
    PRIMARY KEY (media_item_id, ingest_timestamp)
)
'''
        duckdb.sql(f'DROP TABLE IF EXISTS {miml_table_name}')
        duckdb.sql(miml_create_table_query)
        self._create_log_df(
            miml_table_name,
            ['media_item_id'],
            ['primary_title', 'secondary_title', 'artist_id', 'media_cover_url'],
            all_intervals,
            self._granularity,
            strptime_format,
            self._media_item_metadata_log_output_path,
        )

    def _create_log_df(
        self,
        table_name: str,
        id_column_names: list[str],
        other_column_names: list[str],
        intervals: list[str],
        granularity: str,
        strptime_format: str,
        output_filepath: str,
    ):
        for interval in intervals:
            # NOTE add tests for all queries
            duckdb.execute(
f'''
-- This query is confusing, but essentially we get each distinct row in the current interval
-- and then join it with the previous row for the same ID
WITH ROW_FOR_GRANULARITY AS (
    SELECT
        {', '.join(id_column_names + other_column_names)},
        timestp AS ingest_timestamp,
        row_number() OVER (PARTITION BY {', '.join(id_column_names)} ORDER BY timestp DESC) as r,
    FROM
        enriched_df
    WHERE
        enriched_df.timestp >= strptime($cur_interval, '{strptime_format}')
        AND enriched_df.timestp < (strptime($cur_interval, '{strptime_format}') + interval '1 {granularity}')
),
LATEST_ROW_FOR_GRANULARITY AS (
    SELECT * FROM ROW_FOR_GRANULARITY WHERE r = 1
),
ALL_PREVIOUS_ROWS_FOR_ID AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY {' ,'.join(id_column_names)} ORDER BY ingest_timestamp DESC) as r FROM {table_name}
    -- let's just drop the uneeded rows down later... WHERE {id_column_names} IN (SELECT {id_column_names} FROM LATEST_ROW_FOR_GRANULARITY)
),
PREVIOUS_ROW_FOR_ID AS (
    SELECT * FROM ALL_PREVIOUS_ROWS_FOR_ID WHERE r = 1
),
NEW_OR_UPDATED_ROWS AS (
    SELECT
        {', '.join([f'L.{c}' for c in (id_column_names + other_column_names)])},
        L.ingest_timestamp,
    FROM LATEST_ROW_FOR_GRANULARITY L
    LEFT JOIN PREVIOUS_ROW_FOR_ID P ON {' AND '.join([f'L.{c} = P.{c}' for c in id_column_names])}
    WHERE
        -- NOTE need to handle nulls here
        P.{id_column_names[0]} IS NULL
        OR
        {' OR '.join([f'L.{c} IS DISTINCT FROM P.{c}' for c in other_column_names])}
)
INSERT INTO {table_name}
(
    {', '.join(id_column_names + other_column_names)},
    ingest_timestamp
)
SELECT
    {', '.join(id_column_names + other_column_names)},
    ingest_timestamp
FROM NEW_OR_UPDATED_ROWS
''', {'cur_interval': interval})
        duckdb.sql(f'from {table_name}').to_csv(output_filepath)

    def csvs_to_pg(self):
        path_to_table_name = {
            self._playlists_output_path: 'playlists',
            self._users_output_path: 'users',
            self._media_items_output_path: 'media_items',
            self._artists_output_path: 'artists',
            self._playlist_metadata_log_output_path: 'playlist_metadata_log',
            self._playlist_plays_log_output_path: 'playlist_plays_log',
            self._playlist_positions_log_output_path: 'playlist_positions_log',
            self._media_item_metadata_log_output_path: 'media_item_metadata_log',
        }
        for path, table_name in path_to_table_name.items():
            df = pd.read_csv(path)
            if table_name == 'playlist_metadata_log':
                df.rename({'playlist_name': 'name'}, axis=1, inplace=True)
            elif table_name == 'media_item_metadata_log':
                df.rename({'media_cover_url': 'cover_url'}, axis=1, inplace=True)
            
            df.to_sql(
                table_name, 
                self._pg_connection_string, 
                if_exists='append', 
                index=False,
            )
