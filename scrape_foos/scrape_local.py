from os import path
import argparse
from datetime import datetime
import string
import re
import warnings
warnings.filterwarnings('ignore')
import urllib.request
from itertools import compress

import lxml.html as LH
import requests
from bs4 import BeautifulSoup as bs

import pandas as pd

from utils import get_request


class bbref_scrape:
    def __init__(self, year, sport_type, url):
        self.year = year
        self.sport_type = sport_type
        self.url = url

    def get_player_info(self):
        r = get_request(self.url)
        all_tags = bs(r.content, "html.parser")

        tbl = all_tags.find("table", attrs={"class" : "sortable stats_table"})
        tbl_rows = tbl.find_all('tr')
        df_columns = ["player", "pos", "age", "tm", "g", "gs", "mp"]
        line = []
        for tr in tbl_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            line.append(row)
        df = pd.DataFrame(line).iloc[1:,0:7]
        df.columns = df_columns
        df = df.set_index("player")

        ids_raw = [x for x in all_tags.find_all("td", class_ = "left")]
        ids = []
        player_name = []
        for x in ids_raw:
            try:
                player_name.append(x.get_text().strip())
                ids.append(x["data-append-csv"])
            except:
                next
        player_id_df = pd.DataFrame({"player":player_name[::2], "bbrefID":ids}).set_index("player")

        df_combined = df.join(player_id_df, how="inner").reset_index()
        gcs_path = "{sport_type}.playerinfo{season}_{partition_date}".format(sport_type=self.sport_type,
                                                                             season=self.year,
                                                                             partition_date=datetime.today().strftime("%Y%m%d"))

        df_combined.to_gbq(project_id='scarlet-labs', destination_table=gcs_path, if_exists="replace")

    def get_player_links(self):
        r = get_request(self.url)
        all_tags = bs(r.content, "html.parser")
        ids_bool = [bool(re.search(pattern="players/\w/.+", string = x["href"])) for x in all_tags.find_all("a")]
        ids = list(compress([x["href"] for x in all_tags.find_all("a")], ids_bool))
        return(list(set([re.sub(pattern="[.](html)",string=x, repl="") for x in ids])))

    def get_player_gamelogs(self, link):
        def text(elt):
            return elt.text_content().replace(u'\xa0', u' ')

        if(self.sport_type == "basketball"):
            ref_link = "basketball-reference.com/"
            n = 30
            tbl_xpath = '//*[@id="pgl_basic"]'
            game_log_cols = ['bbrefID', 'G', 'date', 'age', 'tm', 'is_away', 'opp', 'game_outcome', 'GS', 'MP', 'FG',
                               'FGA', 'FG_pct', 'ThreeP', 'ThreePA', 'ThreeP_pct', 'FT', 'FTA', 'FT_pct', 'ORB', 'DRB',
                                   'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 'plus_minus']
        else:
            ref_link = "hockey-reference.com/"
            n = 29
            tbl_xpath = '//*[@id="gamelog"]'

        bbrefID = re.findall(string=link, pattern="(?<=[/])\w+|\d+")[2]
        url = "https://www."+ ref_link + link + "/gamelog/" + str(self.year)
        r = get_request(url)
        all_tags = LH.fromstring(r.content)

        for table in all_tags.xpath(tbl_xpath):
            header = [text(th) for th in table.xpath('//th')][1:n]
            data = [[text(td) for td in tr.xpath('td')]
                    for tr in table.xpath('//tr')][1:]
            data = [row for row in data if len(row)==len(header)]
            data = pd.DataFrame(data, columns = header)
            df = pd.concat([pd.DataFrame({"bbrefID":[bbrefID for bbref in range(len(data))]}), data], axis=1)

            if df is not None and self.sport_type == "basketball" and df.shape[1] == n:
                df.columns = game_log_cols
                df[["FG", "ThreeP", "TRB", "AST", "STL", "BLK", "TOV"]] = df[["FG", "ThreeP", "TRB", "AST", "STL", "BLK", "TOV"]].astype(float)
                df["dk"] = (1*df["FG"]) + ((1/2)*df["ThreeP"]) + ((5/4)*df["TRB"]) + ((3/2)*df["AST"]) + (2*df["STL"]) + (2*df["BLK"]) + ((1/2)*df["TOV"])
                double_double = pd.Series(df[["FG", "TRB", "AST", "STL", "BLK", "TOV"]].apply(lambda x: sum(x>=10), axis = 1) > 1)
                df["dk"][double_double] += 1.5
                return(df)

    def run(self):
        self.get_player_info()
        player_ids = self.get_player_links()
        player_gamelog_list = [self.get_player_gamelogs(link = x) for x in player_ids]
        return pd.concat([x for x in player_gamelog_list if x is not None], axis=0, ignore_index=True)

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project',
                        dest='project',
                        default = None,
                        help='This is the GCP project you wish to send the data')
    parser.add_argument('--sport_type',
                        dest='sport_type',
                        default = 'basketball',
                        help='This is the sport type (either basketball or hockey)')
    parser.add_argument('--year',
                        dest='year',
                        default='2017',
                        help='Specify the season you want to pull')
    parser.add_argument('--url',
                        dest='url',
                        default = 'https://www.basketball-reference.com/leagues/NBA_YYYY_per_game.html',
                        help='The url we will pull data from')

    args, _ = parser.parse_known_args(argv)


    scraper = bbref_scrape(sport_type=args.sport_type,
                           year=args.year,
                           url=args.url.replace("YYYY", args.year))
    bbref_df = scraper.run()
    gcs_path = "{sport_type}.gamelogs{season}_{partition_date}".format(sport_type=args.sport_type, season=args.year, partition_date=datetime.today().strftime("%Y%m%d"))
    # bbref_df.to_csv(path.join("game_logs", "{sport_type}_{season}_{partition_date}.csv".format(sport_type=args.sport_type, season=args.year, partition_date=datetime.today().strftime("%Y%m%d"))), index=False)
    bbref_df.to_gbq(project_id=args.project, destination_table=gcs_path, if_exists="replace")


if __name__ == '__main__':
    main()
