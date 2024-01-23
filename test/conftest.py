import tempfile

import pytest


TEST_PLAYLIST_YOUTUBE_JSON_STR = '''
[
    {
        "playlist_id": "PLx0sYbCqOb8Q_CLZC2BdBSKEEB59BOPUM",
        "playlist_name": "UK Top 40 Songs This Week 2022 (Top Charts Music)",
        "artwork_url": "https://i.ytimg.com/vi/H5v3kku4y6Q/default.jpg",
        "channel_id": "UClYiXQ2e1DUEPcWbcQtq1NA",
        "views": 615521031,
        "num_videos": 40,
        "timestp": "2022-05-20T12:19:10.516896",
        "video_id": "XXYlFuWEuKI",
        "title": "The Weeknd - Save Your Tears (Official Music Video)",
        "artist_name": "The Weeknd",
        "image_url": "https://i.ytimg.com/vi/XXYlFuWEuKI/default.jpg",
        "track_title": "Save Your Tears",
        "position": 36
    },
    {
        "playlist_id": "PLlYKDqBVDxX3Qf5DWNfXtkxVQCHQId9Lo",
        "playlist_name": "As Melhores Musicas 2022 ♫ Sertanejo, Pop Funk etc. (Playlist Musicas Mais Tocadas 2022)",
        "artwork_url": "https://i.ytimg.com/vi/xzvZfUwy2LQ/default.jpg",
        "channel_id": "UCRnu4ZIsaCCDOJdFKIJgirw",
        "views": 417979069,
        "num_videos": 148,
        "timestp": "2022-05-22T12:59:19.152555",
        "video_id": "qjWEOzaE6Is",
        "title": "Hugo e Guilherme - Da Moral Pros Pobre - DVD Próximo Passo",
        "artist_name": null,
        "image_url": "https://i.ytimg.com/vi/qjWEOzaE6Is/default.jpg",
        "track_title": "Da Moral Pros Pobre",
        "position": 146
    }
]
'''

@pytest.fixture
def youtube_playlist_json_file():
    with tempfile.NamedTemporaryFile(mode='w') as f:
        f.write(TEST_PLAYLIST_YOUTUBE_JSON_STR)
        f.flush()
        yield f.name


@pytest.fixture
def output_directory():
    with tempfile.TemporaryDirectory() as d:
        yield d
