from chartmetric_challenge.constants import ORG_YOUTUBE, TYPE_PLAYLIST
from chartmetric_challenge.youtube_playlist_pg import YoutubePlaylistPgIngestor


VALID_INGESTORS = {
    f'{TYPE_PLAYLIST}_{ORG_YOUTUBE}': YoutubePlaylistPgIngestor,
}
