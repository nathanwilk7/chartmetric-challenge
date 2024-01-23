from chartmetric_challenge.youtube_playlist_pg import YoutubePlaylistPgIngestor

import pandas as pd


def test_youtube_playlist_ingestor_load(youtube_playlist_json_file, output_directory):
    ingestor = YoutubePlaylistPgIngestor(
        'day',
        youtube_playlist_json_file,
        output_directory,
        None,
    )
    df = ingestor.load()
    # NOTE make this better
    assert len(df) == 2


def test_convert_playlist_metadata_log(output_directory):
    # NOTE add this type of test for all tables
    ingestor = YoutubePlaylistPgIngestor(
        'day',
        None,
        output_directory,
        None,
    )
    df = pd.DataFrame([
        # T0 playlist1 video1: original row
        {
            "playlist_id": "p1",
            "playlist_name": "pn1",
            "artwork_url": "au1",
            "channel_id": "c1",
            "views": 1,
            "num_videos": 1,
            "timestp": "2022-05-19T12:00:00.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
        # T1 playlist1 video1: new playlist_name
        {
            "playlist_id": "p1",
            "playlist_name": "pn1_v2",
            "artwork_url": "au1",
            "channel_id": "c1",
            "views": 1,
            "num_videos": 1,
            "timestp": "2022-05-20T12:00:00.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
        # T2 playlist1 video1: new artwork_url
        {
            "playlist_id": "p1",
            "playlist_name": "pn1_v2",
            "artwork_url": "au1_v2",
            "channel_id": "c1",
            "views": 1,
            "num_videos": 1,
            "timestp": "2022-05-21T12:00:00.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
        # T3 playlist1 video1: more num_videos
        {
            "playlist_id": "p1",
            "playlist_name": "pn1_v2",
            "artwork_url": "au1_v2",
            "channel_id": "c1",
            "views": 1,
            "num_videos": 2,
            "timestp": "2022-05-22T12:00:00.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
        # T4 playlist1 video1: ignored channel_id (earlier than other from same day)
        {
            "playlist_id": "p1",
            "playlist_name": "pn1_v2",
            "artwork_url": "au1_v2",
            "channel_id": "c1_v2",
            "views": 1,
            "num_videos": 2,
            "timestp": "2022-05-23T12:00:00.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
        # T4 playlist1 video1: new channel_id (may not be possible IRL)
        {
            "playlist_id": "p1",
            "playlist_name": "pn1_v2",
            "artwork_url": "au1_v2",
            "channel_id": "c1_v3",
            "views": 1,
            "num_videos": 2,
            "timestp": "2022-05-23T12:00:01.000000",
            "video_id": "v1",
            "title": "t1",
            "artist_name": "an1",
            "image_url": "i1",
            "track_title": "tt1",
            "position": 1,
        },
    ])
    ingestor.convert(df)
    result_df = pd.read_csv(f'{output_directory}/playlist_metadata_log.csv')
    users_df = pd.read_csv(f'{output_directory}/users.csv')
    expected_fields_at_times = {
        0: {
            'playlist_name': 'pn1',
        },
        1: {
            'playlist_name': 'pn1_v2',
        },
        2: {
            'playlist_name': 'pn1_v2',
            'cover_url': 'au1_v2',
        },
        3: {
            'playlist_name': 'pn1_v2',
            'cover_url': 'au1_v2',
            'num_media_items': 2,
        },
        4: {
            'playlist_name': 'pn1_v2',
            'cover_url': 'au1_v2',
            'num_media_items': 2,
            'user_id': users_df.query("source_id == 'c1_v3'").iloc[0]['id'],
        },
    }
    assert len(result_df) == 5
    assert_expected_fields_at_times(result_df, expected_fields_at_times)


def assert_expected_fields_at_times(df, expected_fields_at_times):
    for i, (_, row) in enumerate(df.sort_values(by='ingest_timestamp').iterrows()):
        if i in expected_fields_at_times:
            expected_fields = expected_fields_at_times[i]
        for field, value in expected_fields.items():
            assert row[field] == value, f'{field} at time {i} should be {value} but is {row[field]}, row: {row}'
