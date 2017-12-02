
# coding: utf-8

# In[1]:

import lxml.html as LH
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import string
import re
import warnings
import urllib.request
from itertools import compress
warnings.filterwarnings('ignore')


# In[2]:

class HTMLTableParser:
       def parse_url(self, url):
           response = requests.get(url)
           soup = bs(response.text, 'lxml')
           return [(table['id'],self.parse_html_table(table)) for table in soup.find_all('table')]  
   
       def parse_html_table(self, table):
           n_columns = 0
           n_rows=0
           column_names = []
   
           # Find number of rows and columns
           # we also find the column titles if we can
           for row in table.find_all('tr'):
               
               # Determine the number of rows in the table
               td_tags = row.find_all('td')
               if len(td_tags) > 0:
                   n_rows+=1
                   if n_columns == 0:
                       # Set the number of columns for our table
                       n_columns = len(td_tags)
                       
               # Handle column names if we find them
               th_tags = row.find_all('th')
               if len(th_tags) > 0 and len(column_names) == 0:
                   for th in th_tags[1:]:
                       column_names.append(th.get_text())
   
           # Safeguard on Column Titles
           if len(column_names) > 0 and len(column_names) != n_columns:
               raise Exception("Column titles do not match the number of columns")
   
           columns = column_names if len(column_names) > 0 else range(0,n_columns)
           df = pd.DataFrame(columns = columns,
                             index= range(0,n_rows))
           row_marker = 0
           for row in table.find_all('tr'):
               column_marker = 0
               columns = row.find_all('td')
               for column in columns:
                   df.iat[row_marker,column_marker] = column.get_text()
                   column_marker += 1
               if len(columns) > 0:
                   row_marker += 1
                   
           # Convert to float if possible
           for col in df:
               try:
                   df[col] = df[col].astype(float)
               except ValueError:
                   pass
           
           return df


# In[20]:

class bbref_scrape:
    def get_player_ids(url):
        r = requests.get(url)
        all_tags = bs(r.content, "html.parser")
        tmp = [x for x in all_tags.find_all("td", class_ = "left")]
        ids = []
        for x in tmp:
            try:
                ids.append(x["data-append-csv"])
            except:
                next
        return(list(set(ids)))

    def get_player_links(url):
        r = requests.get(url)
        all_tags = bs(r.content, "html.parser")
        ids_bool = [bool(re.search(pattern="players/\w/.+", string = x["href"])) for x in all_tags.find_all("a")]
        ids = list(compress([x["href"] for x in all_tags.find_all("a")], ids_bool))
        return(list(set([re.sub(pattern="[.](html)",string=x, repl="") for x in ids])))
    
    def get_player_gamelogs(sport_type, link, year):
        def text(elt):
            return elt.text_content().replace(u'\xa0', u' ')
        
        if(sport_type == "basketball"):
            ref_link = "basketball-reference.com/"
            n = 30
            tbl_xpath = '//*[@id="pgl_basic"]'
        else:
            ref_link = "hockey-reference.com/"
            n = 29
            tbl_xpath = '//*[@id="gamelog"]'

        bbrefID = re.findall(string=link, pattern="(?<=[/])\w+|\d+")[2]
        url = "https://www."+ ref_link + link + "/gamelog/" + str(year)
        r = requests.get(url)
        all_tags = LH.fromstring(r.content)
    
        for table in all_tags.xpath(tbl_xpath):
            header = [text(th) for th in table.xpath('//th')][1:n]
            data = [[text(td) for td in tr.xpath('td')]  
                    for tr in table.xpath('//tr')][1:]
            data = [row for row in data if len(row)==len(header)]
            data = pd.DataFrame(data, columns = header)
            df = pd.concat([pd.DataFrame({"bbrefID":[bbrefID for bbref in range(len(data))]}), data], axis=1)
            return(df)


# In[4]:

hp = HTMLTableParser()
#NBA
player_pg = "https://www.basketball-reference.com/leagues/NBA_2017_per_game.html"
player_p100 = "https://www.basketball-reference.com/leagues/NBA_2017_per_poss.html"
player_p36 = "https://www.basketball-reference.com/leagues/NBA_2017_per_minute.html"
player_advanced = "https://www.basketball-reference.com/leagues/NBA_2017_advanced.html"
#NHL
skater_basic = "https://www.hockey-reference.com/leagues/NHL_2018_skaters.html"
skater_advanced = "https://www.hockey-reference.com/leagues/NHL_2018_skaters-advanced.html"


# In[219]:

#NBA Game Logs
year = 2018
game_log_cols = ['bbrefID', 'G', 'Date', 'Age', 'Tm', 'is_away', 'Opp', 'game_outcome', 'GS', 'MP', 'FG',
       'FGA', 'FG_pct', '3P', '3PA', '3P_pct', 'FT', 'FTA', 'FT_pct', 'ORB', 'DRB',
       'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 'plus_minus', 'dk']
ids = bbref_scrape.get_player_links(player_pg)
game_logs_df = pd.concat([bbref_scrape.get_player_gamelogs(sport_type = "basketball", link = x, year = year) for x in ids])
#Create Draft Kings Score
game_logs_df[["FG", "3P", "TRB", "AST", "STL", "BLK", "TOV"]] = game_logs_df[["FG", "3P", "TRB", "AST", "STL", "BLK", "TOV"]].astype(float)
game_logs_df["dk"] = (1*game_logs_df["FG"]) + ((1/2)*game_logs_df["3P"]) + ((5/4)*game_logs_df["TRB"]) + ((3/2)*game_logs_df["AST"]) + (2*game_logs_df["STL"]) + (2*game_logs_df["BLK"]) + ((1/2)*game_logs_df["TOV"])
double_double = pd.Series(game_logs_df[["FG", "TRB", "AST", "STL", "BLK", "TOV"]].apply(lambda x: sum(x>=10), axis = 1) > 1)
game_logs_df["dk"][double_double] += 1.5
#Write the csv
game_logs_df.to_csv("nba_game_logs_%s.csv" % str(year), columns=game_log_cols)


# In[24]:

#NHL Skater Game Logs
year = 2018
game_log_cols = ['bbrefID', 'Date', 'G', 'Age', 'Team', 'is_away', 'Opp', 'win_loss', 'goals', 'assists', 'pts',
       'plus_minus', 'pim', 'g_ev', 'g_pp', 'g_sh', 'g_gw', 'a_ev', 'a_pp', 'a_sh', 'shots', 'shooting_pct', 'shifts', 'toi',
       'hits', 'blocks', 'fow', 'fol', 'fo_pct']
ids = bbref_scrape.get_player_links(skater_basic)
game_logs = [bbref_scrape.get_player_gamelogs(sport_type = "hockey", link = x, year = year) for x in ids]
game_logs_new = list(compress(game_logs, [x is not None for x in game_logs]))
game_logs_df = pd.concat(list(compress(game_logs_new, ["Goalie Stats" not in x.columns for x in game_logs_new])))
game_logs_df.columns = game_log_cols
game_logs_df.to_csv("nhl_game_logs_%s.csv" % str(year))

