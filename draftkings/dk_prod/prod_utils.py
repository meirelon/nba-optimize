#dealing with gcs
import os
import pickle
from datetime import datetime
from gcloud import storage
from tempfile import NamedTemporaryFile

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
