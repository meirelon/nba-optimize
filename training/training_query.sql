with
distinct_player_info AS(
select player, bbrefID, pos
from(
select player, bbrefID, pos, g, secs_played, max(g) over(partition by player, bbrefID) as max_g, max(secs_played) over(partition by player, bbrefID) as max_secs_played
from `scarlet-labs.basketball.playerinfo2018_20181025`
where tm != 'TOT'
)
where g = max_g and secs_played = max_secs_played
),

player_logs_full as(
select date(timestamp(date)) as date, bbrefID, player, pos, tm, opp, G, GS, secs_played, venue,
plus_minus,
dk
from `scarlet-labs.basketball.standard2018_20181025`
join(select * from distinct_player_info)
using(bbrefID)
),

todays_games as(
select *
from player_logs_full
where date = date(timestamp('2018-01-15'))
),

opp_30d AS(
select opp, pos, avg(dk) as dk_against_30d
from player_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 30 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1,2
),

opp_7d AS(
select opp, pos, avg(dk) as dk_against_7d
from player_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 8 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1,2
),

player_30d as(
select bbrefID,
avg(dk) as dk_30d,
avg(secs_played) as secs_played_30d,
sum(if(venue = 'home', 1, 0)) as home_30d,
sum(if(venue = 'away', 1, 0)) as away_30d
from player_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 30 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1
),

player_7d as(
select bbrefID,
avg(dk) as dk_7d,
avg(secs_played) as secs_played_7d,
sum(if(venue = 'home', 1, 0)) as home_7d,
sum(if(venue = 'away', 1, 0)) as away_7d
from player_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 8 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1
)

select *
from todays_games
left join(select * from player_30d)
using(bbrefID)
left join(select * from player_7d)
using(bbrefID)
left join(select * from opp_30d)
using(opp, pos)
left join(select * from opp_7d)
using(opp, pos)
