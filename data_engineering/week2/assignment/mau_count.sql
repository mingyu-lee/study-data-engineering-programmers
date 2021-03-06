-- MAU(Monthly Active User) 구하기
select
    to_char(ST.TS, 'YYYY-MM') as YEAR_MONTH
  , count(distinct(USC.USERID)) as ACTIVE_USER
from RAW_DATA.USER_SESSION_CHANNEL         USC
     inner join RAW_DATA.SESSION_TIMESTAMP ST on USC.SESSIONID = ST.SESSIONID
group by 1
order by 1
;
