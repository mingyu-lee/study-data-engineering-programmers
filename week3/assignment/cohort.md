# 3주차 과제-4: 코호트 분석


```python
# 데이터베이스 연결
import sqlalchemy
import configparser
from sqlalchemy.types import Integer

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

engine = sqlalchemy.create_engine(sql_conn_str)

%load_ext sql
%sql $sql_conn_str
```

#### 처음 제출한 SQL


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



## 3주차 과제-4 피드백: 코호트 분석 결과 테이블 생성

### 피드백
* 결과 데이터가 정확하지 않음
* 컬럼을 수동으로 생성해줘야 함
* 과제는 테이블까지 생성하는 것임

### 최종 제출
* 리뷰로 받은 다음의 SQL문을 바탕으로 Pandas를 사용하여 pivot 처리하여 테이블 생성
```sql
select
    COHORT_MONTH
  , DATEDIFF(month, COHORT_MONTH, VISITED_MONTH) + 1 as MONTH_N
  , COUNT(distinct COHORT.USERID)
from (
     select
         USERID
       , MIN(DATE_TRUNC('month', TS)) as COHORT_MONTH
     from RAW_DATA.USER_SESSION_CHANNEL   USC
          join RAW_DATA.SESSION_TIMESTAMP T on T.SESSIONID = USC.SESSIONID
     group by 1
     )      COHORT
     join (
          select distinct
              USERID
            , DATE_TRUNC('month', TS) as VISITED_MONTH
          from RAW_DATA.USER_SESSION_CHANNEL   USC
               join RAW_DATA.SESSION_TIMESTAMP T on T.SESSIONID = USC.SESSIONID
          ) VISIT on COHORT.COHORT_MONTH <= VISIT.VISITED_MONTH and COHORT.USERID = VISIT.USERID
group by 1, 2
order by 1, 2
;
```

#### SQL 조회 결과
---


```python
%%sql
select
    COHORT_MONTH
  , DATEDIFF(month, COHORT_MONTH, VISITED_MONTH) + 1 as MONTH_N
  , COUNT(distinct COHORT.USERID)
from (
     select
         USERID
       , MIN(DATE_TRUNC('month', TS)) as COHORT_MONTH
     from RAW_DATA.USER_SESSION_CHANNEL   USC
          join RAW_DATA.SESSION_TIMESTAMP T on T.SESSIONID = USC.SESSIONID
     group by 1
     )      COHORT
     join (
          select distinct
              USERID
            , DATE_TRUNC('month', TS) as VISITED_MONTH
          from RAW_DATA.USER_SESSION_CHANNEL   USC
               join RAW_DATA.SESSION_TIMESTAMP T on T.SESSIONID = USC.SESSIONID
          ) VISIT on COHORT.COHORT_MONTH <= VISIT.VISITED_MONTH and COHORT.USERID = VISIT.USERID
group by 1, 2
order by 1, 2
;

```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    28 rows affected.





<table>
    <tr>
        <th>cohort_month</th>
        <th>month_n</th>
        <th>count</th>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>1</td>
        <td>281</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>2</td>
        <td>262</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>3</td>
        <td>237</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>4</td>
        <td>229</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>5</td>
        <td>224</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>6</td>
        <td>213</td>
    </tr>
    <tr>
        <td>2019-05-01 00:00:00</td>
        <td>7</td>
        <td>206</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>1</td>
        <td>197</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>2</td>
        <td>175</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>3</td>
        <td>160</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>4</td>
        <td>150</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>5</td>
        <td>148</td>
    </tr>
    <tr>
        <td>2019-06-01 00:00:00</td>
        <td>6</td>
        <td>145</td>
    </tr>
    <tr>
        <td>2019-07-01 00:00:00</td>
        <td>1</td>
        <td>211</td>
    </tr>
    <tr>
        <td>2019-07-01 00:00:00</td>
        <td>2</td>
        <td>189</td>
    </tr>
    <tr>
        <td>2019-07-01 00:00:00</td>
        <td>3</td>
        <td>175</td>
    </tr>
    <tr>
        <td>2019-07-01 00:00:00</td>
        <td>4</td>
        <td>167</td>
    </tr>
    <tr>
        <td>2019-07-01 00:00:00</td>
        <td>5</td>
        <td>155</td>
    </tr>
    <tr>
        <td>2019-08-01 00:00:00</td>
        <td>1</td>
        <td>84</td>
    </tr>
    <tr>
        <td>2019-08-01 00:00:00</td>
        <td>2</td>
        <td>73</td>
    </tr>
    <tr>
        <td>2019-08-01 00:00:00</td>
        <td>3</td>
        <td>71</td>
    </tr>
    <tr>
        <td>2019-08-01 00:00:00</td>
        <td>4</td>
        <td>69</td>
    </tr>
    <tr>
        <td>2019-09-01 00:00:00</td>
        <td>1</td>
        <td>17</td>
    </tr>
    <tr>
        <td>2019-09-01 00:00:00</td>
        <td>2</td>
        <td>14</td>
    </tr>
    <tr>
        <td>2019-09-01 00:00:00</td>
        <td>3</td>
        <td>13</td>
    </tr>
    <tr>
        <td>2019-10-01 00:00:00</td>
        <td>1</td>
        <td>150</td>
    </tr>
    <tr>
        <td>2019-10-01 00:00:00</td>
        <td>2</td>
        <td>124</td>
    </tr>
    <tr>
        <td>2019-11-01 00:00:00</td>
        <td>1</td>
        <td>9</td>
    </tr>
</table>



#### 최종 제출 Pandas 프로그래밍
---



```python
# 코호트 분석 sql 결과 DataFrame 세팅
cohort_result = %sql SELECT cohort_month, DATEDIFF(month, cohort_month, visited_month)+1 month_N, COUNT(DISTINCT cohort.userid) FROM ( SELECT userid, MIN(DATE_TRUNC('month', ts)) cohort_month FROM raw_data.user_session_channel usc JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid GROUP BY 1 ) cohort JOIN ( SELECT DISTINCT userid, DATE_TRUNC('month', ts) visited_month FROM raw_data.user_session_channel usc JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid ) visit ON cohort.cohort_month <= visit.visited_month and cohort.userid = visit.userid GROUP BY 1, 2 ORDER BY 1, 2 ;
cohort_data_frame = cohort_result.DataFrame()

# head 확인
cohort_data_frame.head()

# pivot
pivoted_cohort = cohort_data_frame.pivot_table(index='cohort_month', columns='month_n', values='count')

# DateTime 형태의 index를 연월 문자열로 바꾸기 위해 index 리스트 세팅
date_indexes = [index.strftime('%Y-%m-%d') for index in pivoted_cohort.index]

# decimal 형태의 count 값을 integer로 바꾸기 위해 컬럼 데이터타입 세팅
columns = {column:Integer() for column in pivoted_cohort.columns}

# index를 연월 문자열로 세팅
pivoted_cohort = pivoted_cohort.reindex(date_indexes)

# pivot된 index를 컬럼화
pivoted_cohort = pivoted_cohort.reset_index()

# pivot_cohort DataFrame을 데이터베이스 테이블 생성하여 데이터 입력
pivoted_cohort.to_sql('cohort_month', con=engine, if_exists='replace', index=False, index_label='cohort_month', dtype=columns)

# 생성된 테이블 내용 조회
engine.execute("SELECT * FROM cohort_month").fetchall()
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    28 rows affected.





    [('2019-05-01', 281, 262, 237, 229, 224, 213, 206),
     ('2019-06-01', 197, 175, 160, 150, 148, 145, None),
     ('2019-07-01', 211, 189, 175, 167, 155, None, None),
     ('2019-08-01', 84, 73, 71, 69, None, None, None),
     ('2019-09-01', 17, 14, 13, None, None, None, None),
     ('2019-10-01', 150, 124, None, None, None, None, None),
     ('2019-11-01', 9, None, None, None, None, None, None)]




```python
%%sql
SELECT * FROM cohort_month
;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    7 rows affected.





<table>
    <tr>
        <th>cohort_month</th>
        <th>1</th>
        <th>2</th>
        <th>3</th>
        <th>4</th>
        <th>5</th>
        <th>6</th>
        <th>7</th>
    </tr>
    <tr>
        <td>2019-05-01</td>
        <td>281</td>
        <td>262</td>
        <td>237</td>
        <td>229</td>
        <td>224</td>
        <td>213</td>
        <td>206</td>
    </tr>
    <tr>
        <td>2019-06-01</td>
        <td>197</td>
        <td>175</td>
        <td>160</td>
        <td>150</td>
        <td>148</td>
        <td>145</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2019-07-01</td>
        <td>211</td>
        <td>189</td>
        <td>175</td>
        <td>167</td>
        <td>155</td>
        <td>None</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2019-08-01</td>
        <td>84</td>
        <td>73</td>
        <td>71</td>
        <td>69</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2019-09-01</td>
        <td>17</td>
        <td>14</td>
        <td>13</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2019-10-01</td>
        <td>150</td>
        <td>124</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2019-11-01</td>
        <td>9</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
    </tr>
</table>


