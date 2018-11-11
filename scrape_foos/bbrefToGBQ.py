import argparse
from datetime import datetime
import numpy as np
import pandas as pd
from playerinfo_utils import get_player_info, get_player_game_logs

class bbrefToGBQ:
    def __init__(self, project_id, dataset_id, year, nchunks):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.year = year
        self.nchunks = nchunks


    def run(self):
        dt_now = datetime.today().strftime("%Y%m%d")
        player_info = get_player_info(year=self.year)
        player_info_gcs_path = "{dataset_id}.playerinfo{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=dt_now)
        player_info.to_gbq(project_id=self.project_id,
                           destination_table=player_info_gcs_path,
                           if_exists="replace",
                           verbose=False)

        standard_game_logs = [get_player_game_logs(bbrefID=id, year=self.year, game_log_type="standard")
                              for id in np.unique(player_info['bbrefID'])]
        standard_game_logs_df = pd.concat([x for x in standard_game_logs if x is not None], axis=0, ignore_index=True)
        standard_gcs_path = "{dataset_id}.standard{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=dt_now)
        standard_game_logs_df.to_gbq(project_id=self.project_id,
                                     destination_table=standard_gcs_path,
                                     if_exists="replace",
                                     chunksize=self.nchunks,
                                     verbose=False)

        advanced_game_logs = [get_player_game_logs(bbrefID=id, year=self.year, game_log_type="advanced")
                              for id in np.unique(player_info['bbrefID'])]
        advanced_game_logs_df = pd.concat([x for x in advanced_game_logs if x is not None], axis=0, ignore_index=True)
        advanced_gcs_path = "{dataset_id}.advanced{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=dt_now)
        advanced_game_logs_df.to_gbq(project_id=self.project_id,
                                     destination_table=advanced_gcs_path,
                                     if_exists="replace",
                                     chunksize=self.nchunks,
                                     verbose=False)

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project',
                        dest='project',
                        default = None,
                        help='This is the GCP project you wish to send the data')
    parser.add_argument('--dataset_id',
                        dest='dataset_id',
                        default = 'basketball',
                        help='This is the sport type (for now)')
    parser.add_argument('--year',
                        dest='year',
                        default='2019',
                        help='Specify the season you want to pull')
    parser.add_argument('--nchunks',
                        dest='nchunks',
                        default=200,
                        help='total chunks for streaming data to bq')

    args, _ = parser.parse_known_args(argv)


    pipeline = bbrefToGBQ(project_id=args.project,
                          dataset_id=args.dataset_id,
                          year=args.year,
                          nchunks=args.nchunks)
    pipeline.run()

if __name__ == '__main__':
    main()
