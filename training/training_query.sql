with
distinct_player_info AS(
select player, bbrefID, pos
from(
select player, bbrefID, pos, g, MP, max(g) over(partition by player, bbrefID) as max_g, max(MP) over(partition by player, bbrefID) as max_mp
from `scarlet-labs.basketball.playerinfo2018_20181024`
where tm != 'TOT'
)
where g = max_g and MP = max_mp
),

player_logs_full as(
select date(timestamp(date)) as date, bbrefID, player, pos, tm, opp, G, GS, MP,
case when is_away is null then 'home' else 'away' end as is_away,
plus_minus,
dk
from `scarlet-labs.basketball.gamelogs2018_20181024`
join(select * from distinct_player_info)
using(bbrefID)
),

opp_30d AS(
select opp, pos, is_away, avg(dk) as dk_against_30d
from player_logs_full
where date between date_sub(date, interval 30 day) and date_sub(date, interval 1 day)
group by 1,2,3
),

opp_7d AS(
select opp, pos, is_away, avg(dk) as dk_against_7d
from player_logs_full
where date between date_sub(date, interval 8 day) and date_sub(date, interval 1 day)
group by 1,2,3
),

todays_games as(
select *
from player_logs_full
where date = date(timestamp('2018-01-15'))
),

player_30d as(
select bbrefID, avg(dk) as dk_30d,
sum(if(is_away = 'home', 1, 0)) as home_30d,
sum(if(is_away = 'away', 1, 0)) as away_30d
from player_logs_full
where date between date_sub(date, interval 30 day) and date
group by 1
)

select *
from player_30d
