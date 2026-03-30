--Top 5 teams with most wins

SELECT winner, COUNT(*) as total_wins
FROM matches
WHERE winner != ''
GROUP BY winner
ORDER BY total_wins DESC
LIMIT 5

-- Query 2 — Which season had the most matches?

select count(*) as highest_match_season,season from matches
group by season
order by highest_match_season
limit 1;

--Query 3 — Top 3 players who won most Man of the Match awards

with n1 as(
select count(player_of_match) as highest 
,player_of_match from matches
group by player_of_match
),
n2 AS (
select highest,player_of_match,
dense_rank()
over (order by highest desc) as rnk from n1
)
select highest,player_of_match from n2 where 
rnk <=3
order by player_of_match

--Query 4 — How many matches were decided by DL method per season

with n1 AS(
select count(venue) as highest_count,venue from matches
group by venue
)
select max(highest_count),venue
from n1

--Query 5 — Which venue hosted the most matches

select sum(dl_applied),season
from matches 
where dl_applied=1
group by season




