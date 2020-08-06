```python
# 데이터베이스 연결
import sqlalchemy
import configparser

### psycopg2 install 에러 핸들링
#### env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip --no-cache install psycopg2
import psycopg2

config = configparser.ConfigParser()
config.read('../../config.ini')

user = config['REDSHIFT']['USER']
password = config['REDSHIFT']['PASSWORD']

sql_conn_str = 'postgresql://{user}:{password}@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev'.format(
  user=user,
  password=password
)

sqlalchemy.create_engine(sql_conn_str)

%load_ext sql
%sql $sql_conn_str
```


```python
%%sql
with USER_VISIT_FIRST_MONTH as (
           select
               USERID
             , TO_CHAR(MIN(TS::DATE), 'YYYYMM') as FIRST_VISIT_MONTH
           from RAW_DATA.USER_SESSION_CHANNEL   USC
                join RAW_DATA.SESSION_TIMESTAMP ST on USC.SESSIONID = ST.SESSIONID
           group by 1
           )
   , DATA as (
             select
                 UVFM.USERID
               , UVFM.FIRST_VISIT_MONTH
               , TO_CHAR(TS::DATE, 'YYYYMM') - UVFM.FIRST_VISIT_MONTH as MONTHS_SINCE_FIRST_VISIT
             from USER_VISIT_FIRST_MONTH UVFM
                  join (
                       select *
                       from RAW_DATA.USER_SESSION_CHANNEL   USC
                            join RAW_DATA.SESSION_TIMESTAMP ST on USC.SESSIONID = ST.SESSIONID
                       )                 UST
                       using (USERID)
             )
select
    TO_CHAR(TO_DATE(FIRST_VISIT_MONTH, 'YYYYMM'), 'YYYY-MM')                as COHORT_MONTH
  , count(distinct USERID)                                                  as MONTH_1
  , count(distinct case when MONTHS_SINCE_FIRST_VISIT >= 1 then USERID end) as MONTH_2
  , count(distinct case when MONTHS_SINCE_FIRST_VISIT >= 2 then USERID end) as MONTH_3
  , count(distinct case when MONTHS_SINCE_FIRST_VISIT >= 3 then USERID end) as MONTH_4
from DATA
group by 1
order by 1
;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    7 rows affected.





<table>
    <tr>
        <th>cohort_month</th>
        <th>month_1</th>
        <th>month_2</th>
        <th>month_3</th>
        <th>month_4</th>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>281</td>
        <td>265</td>
        <td>247</td>
        <td>234</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>197</td>
        <td>175</td>
        <td>162</td>
        <td>153</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>211</td>
        <td>192</td>
        <td>180</td>
        <td>170</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>84</td>
        <td>74</td>
        <td>72</td>
        <td>69</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>17</td>
        <td>14</td>
        <td>13</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>150</td>
        <td>124</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>9</td>
        <td>0</td>
        <td>0</td>
        <td>0</td>
    </tr>
</table>


