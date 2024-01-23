-- NOTE DRY, sqlalchemy, alembic

create type SOURCE as enum ('YOUTUBE', 'SPOTIFY');

create table users (
    -- NOTE handle bigserial starting at 1 even though I'm inserting not at 1...
    id bigserial primary key,
    source SOURCE not null,
    source_id varchar not null,
    unique (source, source_id)
);

create table playlists (
    id bigserial primary key,
    source SOURCE not null,
    source_id varchar not null,
    unique (source, source_id)
);

create table media_items (
    id bigserial primary key,
    source SOURCE not null,
    source_id varchar not null,
    unique (source, source_id)
);


create table artists (
    id bigserial primary key,
    name varchar not null,
    unique (name)
);

create table playlist_metadata_log (
    playlist_id bigint not null references playlists(id),
    ingest_timestamp timestamp not null,
    name varchar,
    cover_url varchar,
    user_id bigint references users(id),
    num_media_items int,
    primary key (playlist_id, ingest_timestamp)
);

create table playlist_plays_log (
    playlist_id bigint not null references playlists(id),
    ingest_timestamp timestamp not null,
    plays bigint,
    primary key (playlist_id, ingest_timestamp)
);

create table playlist_positions_log (
    playlist_id bigint not null references playlists(id),
    media_item_id bigint not null references media_items(id),
    ingest_timestamp timestamp not null,
    position int,
    primary key (playlist_id, media_item_id, ingest_timestamp)
);

create table media_item_metadata_log (
    media_item_id bigint not null references media_items(id),
    ingest_timestamp timestamp not null,
    primary_title varchar,
    secondary_title varchar,
    artist_id bigint references artists(id),
    cover_url varchar,
    primary key (media_item_id, ingest_timestamp)
);

create table isrc_lookup (
    track varchar,
    artist varchar,
    isrc varchar
);
