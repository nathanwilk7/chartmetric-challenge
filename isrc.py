import pandas as pd


isrc_df = pd.read_csv('cm_track.csv')
isrc_df[['track', 'artist', 'isrc']].to_sql(
    'isrc_lookup',
    'postgresql://example:example@localhost:5454/chartmetric_challenge',
    if_exists='append',
    index=False,
)