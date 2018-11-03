from os import path
from datetime import datetime
import string
import re
import warnings
warnings.filterwarnings('ignore')
import urllib.request
# from itertools import compress

import lxml.html as LH
import requests
from bs4 import BeautifulSoup as bs

import numpy as np
import pandas as pd

from utils import get_request



def get_player_info(year):
    url = 'https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html'.format(year=year)
    r = get_request(url)
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
    df_combined = df.join(player_id_df, how="inner").reset_index().drop_duplicates()
    df_combined['mp'] = df_combined['mp'].astype(float)
    # df_combined['secs_played'] = df_combined['mp'].apply(lambda x: (int(x.split(".")[0])*60) + int((x.split(".")[1])))

    return df_combined


def get_player_game_logs(bbrefID, game_log_type, year):
    if game_log_type == "standard":
        url = 'https://www.basketball-reference.com/players/{first_letter}/{bbrefID}/gamelog/{year}/'.format(first_letter = bbrefID[0],
                                                                                                             bbrefID=bbrefID,
                                                                                                             year=year)
        game_log_cols = ['G', 'date', 'age', 'tm', 'venue', 'opp', 'game_outcome', 'GS', 'MP', 'FG',
                           'FGA', 'FG_pct', 'ThreeP', 'ThreePA', 'ThreeP_pct', 'FT', 'FTA', 'FT_pct', 'ORB', 'DRB',
                               'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 'plus_minus']

    elif game_log_type == "advanced":
        url = url = 'https://www.basketball-reference.com/players/{first_letter}/{bbrefID}/gamelog-advanced/{year}/'.format(first_letter = bbrefID[0],
                                                                                                                            bbrefID=bbrefID,
                                                                                                                            year=year)
        game_log_cols = ['G', 'date', 'age', 'tm', 'venue', 'opp', 'game_outcome', 'GS', 'MP', "TS_pct", "eFG_pct", "ORB_pct",
                "DRB_pct", "TRB_pct", "AST_pct", "STL_pct", "BLK_pct", "TOV_pct", "USG_pct", "ORtg", "DRtg", "GmSc"]

    #make request for table
    r = get_request(url)
    all_tags = bs(r.content, "html.parser")
    try:
        tbl = all_tags.find("table", attrs={"class" : "row_summable sortable stats_table"})
        tbl_rows = tbl.find_all('tr')
        line = []
        for tr in tbl_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            line.append(row)

        #clean the table
        df = pd.DataFrame(line).iloc[1:,:]
        df.columns = game_log_cols
        df = df[df['G'] != ''].dropna(axis=0).reset_index().iloc[:,1:]

        if game_log_type == "standard":
            df['secs_played'] = df['MP'].apply(lambda x: (int(x.split(":")[0])*60) + int((x.split(":")[1])))
            df['venue'] = np.where(df['venue'] == '@', 'away', 'home')
            df[["FG", "ThreeP", "TRB", "AST", "STL", "BLK", "TOV"]] = df[["FG", "ThreeP", "TRB", "AST", "STL", "BLK", "TOV"]].astype(float)
            df["dk"] = (1*df["FG"]) + ((1/2)*df["ThreeP"]) + ((5/4)*df["TRB"]) + ((3/2)*df["AST"]) + (2*df["STL"]) + (2*df["BLK"]) + ((1/2)*df["TOV"])
            double_double = pd.Series(df[["FG", "TRB", "AST", "STL", "BLK", "TOV"]].apply(lambda x: sum(x>=10), axis = 1) > 1)
            df["dk"][double_double] += 1.5

        elif game_log_type =="advanced":
            df['venue'] = np.where(df['venue'] == '@', 'away', 'home')

        df['bbrefID'] = [bbrefID for bbref in range(len(df))]
        return df
    except:
        return None
