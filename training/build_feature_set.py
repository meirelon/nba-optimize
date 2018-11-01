import argparse
import pandas as pd
import numpy as np
from training_utils import get_rolling_game_avgs, load_pipeline


class BuildFeatureSet:
    def __init__(self, project, bucket, destination_path, filename, season, partition_date, output_table, is_today=False):
        self.project = project
        self.bucket = bucket
        self.destination_path = destination_path
        self.filename = filename
        self.season = season
        self.partition_date = partition_date
        self.output_table = output_table
        self.is_today = is_today
        self._df = None


    @property
    def get_df(self):
        if not self._df:
            query = load_pipeline(project_id=self.project,
                                  bucket=self.bucket,
                                  destination_path=self.destination_path,
                                  filename=self.filename)
            prepared_query = query.format(season=self.season, partition_date=self.partition_date)
            self._df = pd.read_gbq(query=prepared_query, project_id=self.project, dialect="standard", verbose=False)
            return self._df

    def get_feature_df(self):
        df = self.get_df
        player_feature_list = [get_rolling_game_avgs(df, index_on=['bbrefID'], games=g) for g in [5,10,20]]
        opp_feature_list = [get_rolling_game_avgs(df, index_on=['opp', 'pos'], games=g) for g in [5,10,20]]

        player_feature_df = pd.concat(player_feature_list, axis=1)
        opp_feature_df = pd.concat(opp_feature_list, axis=1).reset_index().groupby(['date', 'opp', 'pos']).median()

        feature_df = df.set_index(['date', 'bbrefID']).join(player_feature_df).reset_index().set_index(['date', 'opp', 'pos']).join(opp_feature_df).reset_index()

        if self.is_today:
            most_recent_df = df.groupby('bbrefID', as_index=False, group_keys=False).max()[['bbrefID', 'date']].reset_index().drop(['index'], axis=1)
            return most_recent_df.set_index(['bbrefID', 'date']).join(feature_df.set_index(['bbrefID', 'date']))
        else:
            return feature_df

    def run(self):
        features = self.get_feature_df()
        features.to_gbq(project_id=self.project,
                        destination_table="{sport_type}.features_{partition_date}".format(sport_type="basketball",
                                                                                                partition_date=self.partition_date),
                                                                                                if_exists="replace",
                                                                                                verbose=False,
                                                                                                chunksize=100)



def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project',
                        dest='project',
                        default = 'scarlet-labs',
                        help='This is the GCP project you wish to send the data')
    parser.add_argument('--bucket',
                        dest='bucket',
                        default = 'draftkings',
                        help='bucket to store train')
    parser.add_argument('--destination_path',
                        dest='destination_path',
                        default = 'training')
    parser.add_argument('--filename',
                        dest='filename',
                        default = 'get_player_data')
    parser.add_argument('--season',
                        dest='season',
                        default = '2018')
    parser.add_argument('--partition_date',
                        dest='partition_date',
                        default = '20181025')
    parser.add_argument('--output_table',
                        dest='output_table',
                        default = 'training')
    parser.add_argument('--is_today',
                        dest='is_today',
                        default = False)

    args, _ = parser.parse_known_args(argv)

    pipeline = BuildFeatureSet(project=args.project,
                                bucket=args.bucket,
                                destination_path=args.destination_path,
                                filename=args.filename,
                                season=args.season,
                                partition_date=args.partition_date,
                                output_table=args.output_table,
                                is_today=args.is_today)

    pipeline.run()

if __name__ == '__main__':
    main()
