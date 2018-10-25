with
distinct_player_info AS(
select player, bbrefID, pos
from(
select player, bbrefID, pos, g, mp, max(g) over(partition by player, bbrefID) as max_g, max(mp) over(partition by player, bbrefID) as max_mp
from `scarlet-labs.basketball.playerinfo2018_20181025`
where tm != 'TOT'
)
where g = max_g and mp = max_mp
),

player_logs_full as(
select date(timestamp(date)) as date, bbrefID, player, pos, tm, opp, G, GS, secs_played, venue,
plus_minus,
dk
from `scarlet-labs.basketball.standard2018_20181025`
join(select * from distinct_player_info)
using(bbrefID)
),

advanced_logs_full AS(
select date(timestamp(date)) as date, bbrefID, player, pos, tm, opp, G, GS, venue,
CAST(TS_pct as float64) as TS_pct,
CAST(eFG_pct as float64) as eFG_pct,
CAST(ORB_pct as float64) as ORB_pct, CAST(DRB_pct as float64) as DRB_pct, CAST(TRB_pct as float64) as TRB_pct, CAST(AST_pct as float64) as AST_pct, CAST(STL_pct as float64) as STL_pct,
CAST(BLK_pct as float64) as BLK_pct, CAST(TOV_pct as float64) as TOV_pct, CAST(USG_pct as float64) as USG_pct,
cast(ORtg as float64) as ORtg, cast(DRtg as float64) as DRtg, cast(GmSc as float64) as GmSc
from `scarlet-labs.basketball.advanced2018_20181025`
join(select * from distinct_player_info)
using(bbrefID)
),

todays_games as(
select date, player, bbrefID, pos, tm, opp, venue, dk
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

opp_adv_30d AS(
select opp, pos, avg(TS_pct) as opp_tspct_30d, avg(eFG_pct) as opp_efgpct_30d, avg(orb_pct) as opp_orbpct_30d, avg(drb_pct) as opp_drbpct_30d,
avg(AST_pct) as opp_astpct_30d, avg(STL_pct) as opp_stlpct_30d, avg(BLK_pct) as opp_blkpct_30d, avg(TOV_pct) as opp_tovpct_30d, avg(USG_pct) as opp_usgpct_30d,
avg(ORtg) as opp_ortg_30d, avg(DRtg) as opp_drtg_30d, avg(GmSc) as opp_gmsc_30d
from advanced_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 30 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1,2
),

opp_adv_7d AS(
select opp, pos, avg(TS_pct) as opp_tspct_7d, avg(eFG_pct) as opp_efgpct_7d, avg(orb_pct) as opp_orbpct_7d, avg(drb_pct) as opp_drbpct_7d,
avg(AST_pct) as opp_astpct_7d, avg(STL_pct) as opp_stlpct_7d, avg(BLK_pct) as opp_blkpct_7d, avg(TOV_pct) as opp_tovpct_7d, avg(USG_pct) as opp_usgpct_7d,
avg(ORtg) as opp_ortg_7d, avg(DRtg) as opp_drtg_7d, avg(GmSc) as opp_gmsc_7d
from advanced_logs_full
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
),

player_adv_30d AS(
select bbrefID, avg(TS_pct) as tspct_30d, avg(eFG_pct) as efgpct_30d, avg(orb_pct) as orbpct_30d, avg(drb_pct) as drbpct_30d,
avg(AST_pct) as astpct_30d, avg(STL_pct) as stlpct_30d, avg(BLK_pct) as blkpct_30d, avg(TOV_pct) as tovpct_30d, avg(USG_pct) as usgpct_30d,
avg(ORtg) as ortg_30d, avg(DRtg) as drtg_30d, avg(GmSc) as gmsc_30d
from advanced_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 30 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1
),

player_adv_7d AS(
select bbrefID, avg(TS_pct) as tspct_7d, avg(eFG_pct) as efgpct_7d, avg(orb_pct) as orbpct_7d, avg(drb_pct) as drbpct_7d,
avg(AST_pct) as astpct_7d, avg(STL_pct) as stlpct_7d, avg(BLK_pct) as blkpct_7d, avg(TOV_pct) as tovpct_7d, avg(USG_pct) as usgpct_7d,
avg(ORtg) as ortg_7d, avg(DRtg) as drtg_7d, avg(GmSc) as gmsc_7d
from advanced_logs_full
where date between date_sub(date(timestamp('2018-01-15')), interval 8 day) and date_sub(date(timestamp('2018-01-15')), interval 1 day)
group by 1
)

select *
from todays_games
left join(select * from player_30d)
using(bbrefID)
left join(select * from player_7d)
using(bbrefID)
left join(select * from player_adv_30d)
using(bbrefID)
left join(select * from player_adv_7d)
using(bbrefID)
left join(select * from opp_30d)
using(opp, pos)
left join(select * from opp_7d)
using(opp, pos)
left join(select * from opp_adv_30d)
using(opp, pos)
left join(select * from opp_adv_7d)
using(opp, pos)
