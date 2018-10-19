import argparse
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

    def get_player_ids(self):
        r = get_request(self.url)
        all_tags = bs(r.content, "html.parser")
        tmp = [x for x in all_tags.find_all("td", class_ = "left")]
        ids = []
        for x in tmp:
            try:
                ids.append(x["data-append-csv"])
            except:
                next
        return(list(set(ids)))

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
            game_log_cols = ['bbrefID', 'G', 'Date', 'Age', 'Tm', 'is_away', 'Opp', 'game_outcome', 'GS', 'MP', 'FG',
                               'FGA', 'FG_pct', '3P', '3PA', '3P_pct', 'FT', 'FTA', 'FT_pct', 'ORB', 'DRB',
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

            if df is not None and self.sport_type == "basketball":
                df.columns = game_log_cols
                df[["FG", "3P", "TRB", "AST", "STL", "BLK", "TOV"]] = df[["FG", "3P", "TRB", "AST", "STL", "BLK", "TOV"]].astype(float)
                df["dk"] = (1*df["FG"]) + ((1/2)*df["3P"]) + ((5/4)*df["TRB"]) + ((3/2)*df["AST"]) + (2*df["STL"]) + (2*df["BLK"]) + ((1/2)*df["TOV"])
                double_double = pd.Series(df[["FG", "TRB", "AST", "STL", "BLK", "TOV"]].apply(lambda x: sum(x>=10), axis = 1) > 1)
                df["dk"][double_double] += 1.5
            return(df)

    def run(self):
        player_ids = self.get_player_links()
        player_gamelog_list = [self.get_player_gamelogs(link = x) for x in player_ids[0:5]]
        return pd.concat([x for x in player_gamelog_list if x is not None], axis=0, ignore_index=True)

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--sport_type',
                        dest='sport_type',
                        default = 'basketball',
                        help='This is the sport type (either basketball or hockey)')
    parser.add_argument('--year',
                        dest='year',
                        help='Specify the season you want to pull')
    parser.add_argument('--url',
                        dest='url',
                        default = 'https://www.basketball-reference.com/leagues/NBA_YYYY_per_game.html',
                        help='The url we will pull data from')

    args, _ = parser.parse_known_args(argv)


    scraper = bbref_scrape(sport_type=args.sport_type,
                           year=args.year,
                           url=args.url)
    bbref_df = scraper.run()
    # bbref_df.to_gbq(project_id="scarlet-labs", destination_table="basketball.gamelogs_2017")


if __name__ == '__main__':
    main()
