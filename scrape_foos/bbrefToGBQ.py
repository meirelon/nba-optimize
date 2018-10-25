import argparse
from datetime import datetime
import numpy as np
import pandas as pd
from playerinfo_util import get_player_info, get_player_game_logs

class bbrefToGBQ:
    def __init__(self, project_id, dataset_id, year):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.year =year

    def run(self):
        player_info = get_player_info(year=self.year)
        player_info_gcs_path = "{dataset_id}.playerinfo{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=datetime.today().strftime("%Y%m%d"))
        player_info.to_gbq(project_id=self.project_id,
                           destination_table=player_info_gcs_path,
                           if_exists="replace")

        standard_game_logs = [get_player_game_logs(bbrefID=id, year=self.year, game_log_type="standard")
                              for id in np.unique(player_info['bbrefID'])]
        standard_game_logs_df = pd.concat([x for x in standard_game_logs if x is not None], axis=0, ignore_index=True)
        standard_gcs_path = "{dataset_id}.standard{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=datetime.today().strftime("%Y%m%d"))
        standard_game_logs_df.to_gbq(project_id=self.project_id,
                                     destination_table=standard_gcs_path,
                                     if_exists="replace")

        advanced_game_logs = [get_player_game_logs(bbrefID=id, year=self.year, game_log_type="advanced")
                              for id in np.unique(player_info['bbrefID'])]
        advanced_game_logs_df = pd.concat([x for x in advanced_game_logs if x is not None], axis=0, ignore_index=True)
        advanced_gcs_path = "{dataset_id}.advanced{season}_{partition_date}".format(dataset_id=self.dataset_id,
                                                                           season=self.year,
                                                                           partition_date=datetime.today().strftime("%Y%m%d"))
        advanced_game_logs_df.to_gbq(project_id=self.project_id,
                                     destination_table=advanced_gcs_path,
                                     if_exists="replace")

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
                        default='2018',
                        help='Specify the season you want to pull')

    args, _ = parser.parse_known_args(argv)


    pipeline = bbrefToGBQ(project_id=args.project,
                          dataset_id=args.dataset_id,
                          year=args.year)
    pipeline.run()

if __name__ == '__main__':
    main()
