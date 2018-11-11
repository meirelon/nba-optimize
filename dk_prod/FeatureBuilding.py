import argparse
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from prod_utils import get_rolling_game_avgs, load_pipeline


class BuildFeatureSet:
    def __init__(self, project, bucket, destination_path, filename, season, partition_date, is_today=False):
        self.project = project
        self.bucket = bucket
        self.destination_path = destination_path
        self.filename = filename
        self.season = season
        self.is_today = is_today
        self.partition_date = partition_date
        self._df = None


    @property
    def get_partition_date(self):
        if not self.partition_date:
            tmp_ = datetime.today()- timedelta(days=1)
            self.partition_date = tmp_.strftime("%Y%m%d")
            return self.partition_date
        else:
            return self.partition_date

    @property
    def get_df(self):
        if not self._df:
            query = load_pipeline(project_id=self.project,
                                  bucket=self.bucket,
                                  destination_path=self.destination_path,
                                  filename=self.filename)
            prepared_query = query.format(season=self.season, partition_date=self.get_partition_date)
            self._df = pd.read_gbq(query=prepared_query, project_id=self.project, dialect="standard", verbose=False)
            return self._df

    def get_feature_df(self):
        df = self.get_df
        player_feature_list = [get_rolling_game_avgs(df, index_on=['bbrefID'], games=g) for g in [5,10]]
        opp_feature_list = [get_rolling_game_avgs(df, index_on=['opp', 'pos'], games=g) for g in [5,10]]

        player_feature_df = pd.concat(player_feature_list, axis=1)
        opp_feature_df = pd.concat(opp_feature_list, axis=1).reset_index().groupby(['date', 'opp', 'pos']).median()

        feature_df = df.set_index(['date', 'bbrefID']).join(player_feature_df).reset_index().set_index(['date', 'opp', 'pos']).join(opp_feature_df).reset_index()
        feature_df = feature_df.drop(["secs_played", "plus_minus", "TS_pct", "eFG_pct", "ORB_pct", "DRB_pct", "TRB_pct", "AST_pct", "STL_pct", "BLK_pct", "TOV_pct", "USG_pct", "ORtg", "DRtg", "GmSc"], axis=1)

        if self.is_today:
            most_recent_df = df.groupby('bbrefID', as_index=False, group_keys=False).max()[['bbrefID', 'date']].reset_index().drop(['index'], axis=1)
            return most_recent_df.set_index(['bbrefID', 'date']).join(feature_df.set_index(['bbrefID', 'date'])).reset_index()
        else:
            return feature_df

    def to_gbq(self):
        features = self.get_feature_df()
        features.to_gbq(project_id=self.project,
                        destination_table="{sport_type}.training{season}_{partition_date}".format(sport_type="basketball",
                                                                                                  season=self.season,
                                                                                                partition_date=self.get_partition_date),
                                                                                                if_exists="replace",
                                                                                                verbose=False,
                                                                                                chunksize=1000)



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
                        default = 'sql_queries/training')
    parser.add_argument('--filename',
                        dest='filename',
                        default = 'get_player_data')
    parser.add_argument('--season',
                        dest='season',
                        default = '2019')
    parser.add_argument('--partition_date',
                        dest='partition_date',
                        default = None)
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
                                is_today=args.is_today)

    pipeline.to_gbq()

if __name__ == '__main__':
    main()
