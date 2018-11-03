#dealing with gcs
import os
import pickle
from datetime import datetime
from gcloud import storage
from tempfile import NamedTemporaryFile

import pandas as pd

def load_pipeline(project_id, bucket, destination_path, filename, local=False):
    if local:
        client = storage.Client.from_service_account_json(project=project_id,
                                                          json_credentials_path='../scarlet-labs-2e06fe082fb4.json')
    else:
        client = storage.Client(project=project_id)

    with NamedTemporaryFile(mode='rb') as tempfile:
        gcs_path = os.path.join(destination_path, '{filename}.pkl'.format(filename=filename))
        client.bucket(bucket).blob(gcs_path).download_to_filename(tempfile.name)
        tempfile.seek(0)
        return pickle.load(tempfile)

def get_rolling_game_avgs(df, index_on, games=20):
    # Given a dataframe and an index and number of games, compute the rolling averages (do this for player and opponent/pos)
    _df = df.groupby(index_on, as_index=False, group_keys=False).rolling(games).mean().reset_index().drop(["index"], axis=1).fillna(0)
    df_transformed = _df.set_index(["date"] + index_on).select_dtypes([np.number])
    new_col_names = ["{col}_{index_type}_{rolling}g".format(col=c,
                                                            index_type = '_'.join(index_on),
                                                            rolling=games) for c in df_transformed.columns]
    df_transformed.columns = new_col_names
    return df_transformed
