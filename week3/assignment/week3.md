```python
# 데이터베이스 연결
import sqlalchemy
import configparser

### psycopg2 install 에러 핸들링
#### env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip --no-cache install psycopg2
import psycopg2

config = configparser.ConfigParser()
config.read('../config.ini')

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
-- 데이터베이스 연결 테스트
SELECT * FROM raw_data.session_timestamp LIMIT 10
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    10 rows affected.





<table>
    <tr>
        <th>sessionid</th>
        <th>ts</th>
    </tr>
    <tr>
        <td>7cdace91c487558e27ce54df7cdb299c</td>
        <td>2019-05-01 00:13:11.783000</td>
    </tr>
    <tr>
        <td>94f192dee566b018e0acf31e1f99a2d9</td>
        <td>2019-05-01 00:49:46.073000</td>
    </tr>
    <tr>
        <td>7ed2d3454c5eea71148b11d0c25104ff</td>
        <td>2019-05-01 10:18:43.210000</td>
    </tr>
    <tr>
        <td>f1daf122cde863010844459363cd31db</td>
        <td>2019-05-01 13:10:56.413000</td>
    </tr>
    <tr>
        <td>fd0efcca272f704a760c3b61dcc70fd0</td>
        <td>2019-05-01 13:45:19.793000</td>
    </tr>
    <tr>
        <td>8804f94e16ba5b680e239a554a08f7d2</td>
        <td>2019-05-01 14:23:07.660000</td>
    </tr>
    <tr>
        <td>c5f441cd5f43eb2f2c024e1f8b5d00cd</td>
        <td>2019-05-01 15:03:54.650000</td>
    </tr>
    <tr>
        <td>d5fcc35c94879a4afad61cacca56192c</td>
        <td>2019-05-01 15:13:16.140000</td>
    </tr>
    <tr>
        <td>3d191ef6e236bd1b9bdb9ff4743c47fe</td>
        <td>2019-05-01 15:33:58.197000</td>
    </tr>
    <tr>
        <td>c17028c9b6e0c5deaad29665d582284a</td>
        <td>2019-05-01 15:59:57.490000</td>
    </tr>
</table>



### 실습 과제 1
#### 오늘 살펴본 SQL 실습 및 요약
```sql
-- DDL: CREATE
-- 테이블 생성
create table ADHOC.MINGYU_CHANNEL
(
    CHANNEL VARCHAR(32) primary key
);

-- DML: INSERT
-- 데이터 입력
insert into ADHOC.MINGYU_CHANNEL
values ('FACEBOOK')
     , ('GOOGLE');

-- DDL: DROP
-- 테이블 삭제
drop table ADHOC.MINGYU_CHANNEL;

-- CTAS 기법
-- 셀렉트 조회 내용 스키마대로 TABLE을 만들며 셀렉트 내용을 입력
create table ADHOC.MINGYU_CHANNEL as
select distinct
    CHANNEL
from RAW_DATA.USER_SESSION_CHANNEL
;

-- DDL: 테이블 수정
-- MINGYU_CHANNEL 테이블의 CHANNEL 컬럼 이름을 CHANNELNAME으로 변경
alter table ADHOC.MINGYU_CHANNEL
    rename CHANNEL to CHANNELNAME;

-- TIKTOK 채널 데이터 추가
insert into ADHOC.MINGYU_CHANNEL values ('TIKTOK');

-- Google 또는 Facebook 채널로 접속한 사용자 세션수 조회
select
    COUNT(1)
from RAW_DATA.USER_SESSION_CHANNEL
where CHANNEL in ('Google', 'Facebook');

-- 대소문자 구분 없는 Google 또는 Facebook 채널로 접속한 사용자 세션수 조회
select
    COUNT(1)
from RAW_DATA.USER_SESSION_CHANNEL
where CHANNEL ilike 'Google'
   or CHANNEL like 'Facebook';

-- 대소문자 구분 없이 o가 포함된 채널 종류 구하기
select distinct
    CHANNEL
from RAW_DATA.USER_SESSION_CHANNEL
where CHANNEL ilike '%o%';

-- 대소문자 구분 없이 o가 포함되지 않은 채널 종류 구하기
select distinct
    CHANNEL
from RAW_DATA.USER_SESSION_CHANNEL
where CHANNEL not ilike '%o%';

-- 채널이름 관련 String 함수 처리
-- LEN: 문자열 길이 조회
-- UPPER: 대문자로 변환
-- LOWER: 소문자로 변환
-- LEFT(str, N): 문자열 왼쪽부터 N개를 잘라 반환
-- REPLACE(str, expr1, expr2): 문자열의 expr1 부분을 expr2로 변환
select
    LEN(CHANNELNAME)
  , UPPER(CHANNELNAME)
  , LOWER(CHANNELNAME)
  , LEFT(CHANNELNAME, 4)
  , REPLACE(UPPER(LEFT(CHANNELNAME, 4)), 'OO', 'XX')
from ADHOC.MINGYU_CHANNEL
;

-- 세션이 가장 많이 생기는 시간대 구하기
select
    EXTRACT(hour from TS)
  , count(1) as HOUR_COUNT
from RAW_DATA.SESSION_TIMESTAMP
group by 1
order by 2 desc
limit 1
;

-- 사용자가 가장 많이 생기는 시간대 구하기
select
    EXTRACT(hour from ST.TS)
  , COUNT(distinct (USC.USERID))
from RAW_DATA.USER_SESSION_CHANNEL   USC
     join RAW_DATA.SESSION_TIMESTAMP ST on USC.SESSIONID = ST.SESSIONID
group by 1
order by 2 desc
limit 1;

-- 세션이 가장 많이 생기는 요일 구하기
select
    EXTRACT(dow from TS)
  , count(1) as DOW_COUNT
from RAW_DATA.SESSION_TIMESTAMP
group by 1
order by 2 desc
limit 1
;

-- raw_data.channel 채널별 사용자수 세기
select
    CHANNELNAME
  , count(distinct (USERID)) as USER_COUNT_PER_CHANNEL
from RAW_DATA.CHANNEL                        CA
     left join RAW_DATA.USER_SESSION_CHANNEL USC on CA.CHANNELNAME = USC.CHANNEL
group by 1

-- 251번 사용자의 처음 채널과 마지막 채널 알아내기
-- 정답: Facebook 첫번째, Google 마지막
select
    USERID
  , ROW_NUMBER() over (partition by USERID order by ST.TS) as R
  , MAX() over (partition by USERID order by ST.TS) as MAX_R
from RAW_DATA.USER_SESSION_CHANNEL         USC
     inner join RAW_DATA.SESSION_TIMESTAMP ST on USC.SESSIONID = ST.SESSIONID
where USERID = 251
;
```

## 실습 과제 2
### Gross Revenue 가장 큰 UserID 10개 찾기
* refund 포함

### 기존 제출


```python
%%sql
select
    USERID
  , TOTAL_REVENUE
from (
     select
         USERID
       , sum(case when REFUNDED is true then AMOUNT * -1 else AMOUNT end) as TOTAL_REVENUE
     from RAW_DATA.SESSION_TRANSACTION       ST
          join RAW_DATA.USER_SESSION_CHANNEL USC on USC.SESSIONID = ST.SESSIONID
     group by 1
     ) TOP_USER_REVENUE
order by 2 desc
limit 10
;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    10 rows affected.





<table>
    <tr>
        <th>userid</th>
        <th>total_revenue</th>
    </tr>
    <tr>
        <td>989</td>
        <td>743</td>
    </tr>
    <tr>
        <td>772</td>
        <td>556</td>
    </tr>
    <tr>
        <td>1615</td>
        <td>506</td>
    </tr>
    <tr>
        <td>654</td>
        <td>488</td>
    </tr>
    <tr>
        <td>1651</td>
        <td>463</td>
    </tr>
    <tr>
        <td>973</td>
        <td>438</td>
    </tr>
    <tr>
        <td>262</td>
        <td>422</td>
    </tr>
    <tr>
        <td>2682</td>
        <td>414</td>
    </tr>
    <tr>
        <td>891</td>
        <td>412</td>
    </tr>
    <tr>
        <td>1085</td>
        <td>411</td>
    </tr>
</table>



### 기영님 피드백
> REFUND 필드의 값이 False 이면 netRevenue가 0이 되고 grossRevenue는 amount가 되는 것입니다.
즉 순매출액의 경우에는 환불이 된 경우 0이 되는 것이죠.



```python
%%sql
select
    USERID
  , TOTAL_REVENUE
from (
     select
         USERID
       , sum(case when REFUNDED is false then AMOUNT end) as TOTAL_REVENUE
     from RAW_DATA.SESSION_TRANSACTION       ST
          join RAW_DATA.USER_SESSION_CHANNEL USC on USC.SESSIONID = ST.SESSIONID
     group by 1
     ) TOP_USER_REVENUE
order by 2 desc
limit 10
;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    10 rows affected.





<table>
    <tr>
        <th>userid</th>
        <th>total_revenue</th>
    </tr>
    <tr>
        <td>2562</td>
        <td>None</td>
    </tr>
    <tr>
        <td>1861</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2702</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2590</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2490</td>
        <td>None</td>
    </tr>
    <tr>
        <td>233</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2267</td>
        <td>None</td>
    </tr>
    <tr>
        <td>1665</td>
        <td>None</td>
    </tr>
    <tr>
        <td>538</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2644</td>
        <td>None</td>
    </tr>
</table>



### 최종 제출
* 피드백 쿼리의 결과가 null로 나오는데, order by 절에서 기본으로 nulls first라 sum구문에 else 0 추가
* (order by 2 desc nulls last 해도 되지만 amount 숫자 계산이므로 null 일 경우 0 처리하는게 나은 것 같음)
* 기영님 피드백 쿼리와 기존 제출 쿼리의 결과는 같지만, REFUNDED를 판별하는 과정이 다름
* 기존 제출의 경우 REFUNDED 일 경우 AMOUNT * -1을 통해 (실제 환불하는 것처럼) SUM에서 환불된 경우 AMOUNT 만큼 빼준 것
* 피드백의 경우 단순히 생각해서 REFUNDED 일 때 0이므로, REFUNDED false 인 경우에만 AMOUNT 합계 계산
* 기존 제출문의 경우 NET_REVENUE를 작성한 것이었다. GROSS_REVENUE로 변경 완료


```python
%%sql
select
    USERID
  , GROSS_REVENUE
from (
     select
         USERID
       , sum(nvl(AMOUNT, 0)) as GROSS_REVENUE
     from RAW_DATA.SESSION_TRANSACTION       ST
          join RAW_DATA.USER_SESSION_CHANNEL USC on USC.SESSIONID = ST.SESSIONID
     group by 1
     ) TOP_USER_REVENUE
order by 2 desc
limit 10
;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    10 rows affected.





<table>
    <tr>
        <th>userid</th>
        <th>gross_revenue</th>
    </tr>
    <tr>
        <td>989</td>
        <td>743</td>
    </tr>
    <tr>
        <td>772</td>
        <td>556</td>
    </tr>
    <tr>
        <td>1615</td>
        <td>506</td>
    </tr>
    <tr>
        <td>654</td>
        <td>488</td>
    </tr>
    <tr>
        <td>1651</td>
        <td>463</td>
    </tr>
    <tr>
        <td>973</td>
        <td>438</td>
    </tr>
    <tr>
        <td>262</td>
        <td>422</td>
    </tr>
    <tr>
        <td>1099</td>
        <td>421</td>
    </tr>
    <tr>
        <td>2682</td>
        <td>414</td>
    </tr>
    <tr>
        <td>891</td>
        <td>412</td>
    </tr>
</table>



## 실습과제 3
### 채널별 월 매출액 테이블 만들기
- session_timestamp, user_session_channel, channel, transaction 테이블 사용
- channel에 있는 모든 채널에 대해 구성해야함 (값이 없는 경우라도)
- 아래와 같은 필드로 구성
    - month, channel
    - uniqueUsers (총방문 사용자)
    - paidUsers (구매 사용자)
    - conversionRate(구매사용자 / 총방문 사용자)
    - grossRevenue (refund 포함)
    - netRevenue (refund 제외)


```python
%%sql
-- 채널별 월별 매출액 데이터 구하기
select
    CHANNELNAME
  , YEAR_MONTH
  , UNIQUE_USERS
  , PAID_USERS
  , case when PAID_USERS != 0 then ROUND(100.0 * PAID_USERS / UNIQUE_USERS, 2) end as CONVERSION_RATE
  , GROSS_REVENUE
  , NET_REVENUE
from (
     select
         C.CHANNELNAME
       , left(ST.TS, 7)                                                                     as YEAR_MONTH
       , count(distinct USERID)                                                             as UNIQUE_USERS
       , count(distinct case when AMOUNT is not null and REFUNDED is false then USERID end) as PAID_USERS
       , sum(nvl(AMOUNT, 0))                                                                as GROSS_REVENUE
       , sum(case when REFUNDED is false then AMOUNT else 0 end)                            as NET_REVENUE
     from RAW_DATA.CHANNEL                        C
          left join RAW_DATA.USER_SESSION_CHANNEL USC
                    on C.CHANNELNAME = USC.CHANNEL
          left join RAW_DATA.SESSION_TRANSACTION  STR on USC.SESSIONID = STR.SESSIONID
          left join RAW_DATA.SESSION_TIMESTAMP    ST on USC.SESSIONID = ST.SESSIONID
     group by 1, 2
     order by 1, 2
     ) DATAS
;

-- 매출액이 없는 채널의 경우 월별로 데이터가 생기지 않으므로 채널별, 월별 조회문과 조인
select
    CHANNEL_MONTHS.YEAR_MONTH
  , CHANNEL_MONTHS.CHANNELNAME
  , nvl(UNIQUE_USERS, 0)    as UNIQUE_USERS
  , nvl(PAID_USERS, 0)      as PAID_USERS
  , nvl(CONVERSION_RATE, 0) as CONVERSION_RATE
  , nvl(GROSS_REVENUE, 0)   as GROSS_REVENUE
  , nvl(NET_REVENUE, 0)     as NET_REVENUE
from (
     select
         CHANNELNAME
       , YEAR_MONTH
     from (
          select distinct
              CHANNELNAME
          from RAW_DATA.CHANNEL
          )     C
          cross join
              (
              select distinct
                  left(TS, 7) as YEAR_MONTH
              from RAW_DATA.SESSION_TIMESTAMP
              ) MONTHS
     )     CHANNEL_MONTHS
     left join
         (
         select
             CHANNELNAME
           , YEAR_MONTH
           , UNIQUE_USERS
           , PAID_USERS
           , case when PAID_USERS != 0 then ROUND(100.0 * PAID_USERS / UNIQUE_USERS, 2) end as CONVERSION_RATE
           , GROSS_REVENUE
           , NET_REVENUE
         from (
              select
                  C.CHANNELNAME
                , left(ST.TS, 7)                                                                     as YEAR_MONTH
                , count(distinct USERID)                                                             as UNIQUE_USERS
                , count(distinct case when AMOUNT is not null and REFUNDED is false then USERID end) as PAID_USERS
                , sum(nvl(AMOUNT, 0))                                                                as GROSS_REVENUE
                , sum(case when REFUNDED is false then AMOUNT else 0 end)                            as NET_REVENUE
              from RAW_DATA.CHANNEL                        C
                   left join RAW_DATA.USER_SESSION_CHANNEL USC
                             on C.CHANNELNAME = USC.CHANNEL
                   left join RAW_DATA.SESSION_TRANSACTION  STR on USC.SESSIONID = STR.SESSIONID
                   left join RAW_DATA.SESSION_TIMESTAMP    ST on USC.SESSIONID = ST.SESSIONID
              group by 1, 2
              order by 1, 2
              ) DATAS
         ) DATAS
         on CHANNEL_MONTHS.CHANNELNAME = DATAS.CHANNELNAME
             and CHANNEL_MONTHS.YEAR_MONTH = DATAS.YEAR_MONTH
order by 1, 2
;

-- DB툴에서 진행했던 내용 때문에 CTAS문에서 이미 존재하는 테이블 에러가 나므로 테이블이 있을 경우 DROP
drop table if exists ADHOC.MINGYU_MONTHLY_REVENUE_CHANNEL;

-- 위의 조회문을 CTAS 문으로 월별 채널별 매출액 테이블 생성
create table ADHOC.MINGYU_MONTHLY_REVENUE_CHANNEL as
select
    CHANNEL_MONTHS.YEAR_MONTH
  , CHANNEL_MONTHS.CHANNELNAME
  , nvl(UNIQUE_USERS, 0)    as UNIQUE_USERS
  , nvl(PAID_USERS, 0)      as PAID_USERS
  , nvl(CONVERSION_RATE, 0) as CONVERSION_RATE
  , nvl(GROSS_REVENUE, 0)   as GROSS_REVENUE
  , nvl(NET_REVENUE, 0)     as NET_REVENUE
from (
     select
         CHANNELNAME
       , YEAR_MONTH
     from (
          select distinct
              CHANNELNAME
          from RAW_DATA.CHANNEL
          )     C
          cross join
              (
              select distinct
                  left(TS, 7) as YEAR_MONTH
              from RAW_DATA.SESSION_TIMESTAMP
              ) MONTHS
     )     CHANNEL_MONTHS
     left join
         (
         select
             CHANNELNAME
           , YEAR_MONTH
           , UNIQUE_USERS
           , PAID_USERS
           , case when PAID_USERS != 0 then ROUND(100.0 * PAID_USERS / UNIQUE_USERS, 2) end as CONVERSION_RATE
           , GROSS_REVENUE
           , NET_REVENUE
         from (
              select
                  C.CHANNELNAME
                , left(ST.TS, 7)                                                                     as YEAR_MONTH
                , count(distinct USERID)                                                             as UNIQUE_USERS
                , count(distinct case when AMOUNT is not null and REFUNDED is false then USERID end) as PAID_USERS
                , sum(nvl(AMOUNT, 0))                                                                as GROSS_REVENUE
                , sum(case when REFUNDED is false then AMOUNT else 0 end)                            as NET_REVENUE
              from RAW_DATA.CHANNEL                        C
                   left join RAW_DATA.USER_SESSION_CHANNEL USC
                             on C.CHANNELNAME = USC.CHANNEL
                   left join RAW_DATA.SESSION_TRANSACTION  STR on USC.SESSIONID = STR.SESSIONID
                   left join RAW_DATA.SESSION_TIMESTAMP    ST on USC.SESSIONID = ST.SESSIONID
              group by 1, 2
              order by 1, 2
              ) DATAS
         ) DATAS
         on CHANNEL_MONTHS.CHANNELNAME = DATAS.CHANNELNAME
             and CHANNEL_MONTHS.YEAR_MONTH = DATAS.YEAR_MONTH
order by 1, 2
;

-- 생성된 테이블 데이터 확인
select * from ADHOC.MINGYU_MONTHLY_REVENUE_CHANNEL;
```

     * postgresql://leemingyu05:***@grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev
    43 rows affected.
    49 rows affected.
    Done.
    Done.
    49 rows affected.





<table>
    <tr>
        <th>year_month</th>
        <th>channelname</th>
        <th>unique_users</th>
        <th>paid_users</th>
        <th>conversion_rate</th>
        <th>gross_revenue</th>
        <th>net_revenue</th>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Facebook</td>
        <td>247</td>
        <td>11</td>
        <td>4.45</td>
        <td>1199</td>
        <td>997</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Instagram</td>
        <td>234</td>
        <td>9</td>
        <td>3.85</td>
        <td>959</td>
        <td>770</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Organic</td>
        <td>238</td>
        <td>15</td>
        <td>6.30</td>
        <td>1846</td>
        <td>1571</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Youtube</td>
        <td>244</td>
        <td>10</td>
        <td>4.10</td>
        <td>529</td>
        <td>529</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Google</td>
        <td>412</td>
        <td>13</td>
        <td>3.16</td>
        <td>947</td>
        <td>947</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Naver</td>
        <td>398</td>
        <td>15</td>
        <td>3.77</td>
        <td>1090</td>
        <td>1090</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Facebook</td>
        <td>558</td>
        <td>31</td>
        <td>5.56</td>
        <td>2222</td>
        <td>2144</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Instagram</td>
        <td>567</td>
        <td>24</td>
        <td>4.23</td>
        <td>1896</td>
        <td>1766</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Organic</td>
        <td>557</td>
        <td>24</td>
        <td>4.31</td>
        <td>1600</td>
        <td>1600</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Youtube</td>
        <td>564</td>
        <td>34</td>
        <td>6.03</td>
        <td>2210</td>
        <td>2037</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Google</td>
        <td>610</td>
        <td>23</td>
        <td>3.77</td>
        <td>2210</td>
        <td>1894</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Naver</td>
        <td>626</td>
        <td>19</td>
        <td>3.04</td>
        <td>1829</td>
        <td>1551</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Google</td>
        <td>599</td>
        <td>23</td>
        <td>3.84</td>
        <td>1872</td>
        <td>1691</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Naver</td>
        <td>592</td>
        <td>21</td>
        <td>3.55</td>
        <td>1996</td>
        <td>1996</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Facebook</td>
        <td>698</td>
        <td>28</td>
        <td>4.01</td>
        <td>1650</td>
        <td>1641</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Instagram</td>
        <td>707</td>
        <td>31</td>
        <td>4.38</td>
        <td>2568</td>
        <td>2395</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Organic</td>
        <td>709</td>
        <td>29</td>
        <td>4.09</td>
        <td>2762</td>
        <td>2608</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Youtube</td>
        <td>705</td>
        <td>31</td>
        <td>4.40</td>
        <td>2492</td>
        <td>2319</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Facebook</td>
        <td>688</td>
        <td>26</td>
        <td>3.78</td>
        <td>1678</td>
        <td>1678</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Instagram</td>
        <td>669</td>
        <td>25</td>
        <td>3.74</td>
        <td>2116</td>
        <td>2116</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Organic</td>
        <td>677</td>
        <td>31</td>
        <td>4.58</td>
        <td>2626</td>
        <td>2255</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Youtube</td>
        <td>677</td>
        <td>44</td>
        <td>6.50</td>
        <td>3532</td>
        <td>3331</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Google</td>
        <td>253</td>
        <td>10</td>
        <td>3.95</td>
        <td>580</td>
        <td>580</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>Naver</td>
        <td>237</td>
        <td>10</td>
        <td>4.22</td>
        <td>867</td>
        <td>844</td>
    </tr>
    <tr>
        <td>2019-05</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Facebook</td>
        <td>414</td>
        <td>22</td>
        <td>5.31</td>
        <td>1578</td>
        <td>1578</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Instagram</td>
        <td>410</td>
        <td>20</td>
        <td>4.88</td>
        <td>1462</td>
        <td>1418</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Organic</td>
        <td>416</td>
        <td>12</td>
        <td>2.88</td>
        <td>1129</td>
        <td>940</td>
    </tr>
    <tr>
        <td>2019-06</td>
        <td>Youtube</td>
        <td>400</td>
        <td>17</td>
        <td>4.25</td>
        <td>1042</td>
        <td>1042</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Google</td>
        <td>556</td>
        <td>19</td>
        <td>3.42</td>
        <td>1558</td>
        <td>1385</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>Naver</td>
        <td>553</td>
        <td>19</td>
        <td>3.44</td>
        <td>1547</td>
        <td>1547</td>
    </tr>
    <tr>
        <td>2019-07</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Facebook</td>
        <td>611</td>
        <td>18</td>
        <td>2.95</td>
        <td>1009</td>
        <td>1009</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Instagram</td>
        <td>621</td>
        <td>26</td>
        <td>4.19</td>
        <td>2129</td>
        <td>2001</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Organic</td>
        <td>608</td>
        <td>25</td>
        <td>4.11</td>
        <td>1643</td>
        <td>1606</td>
    </tr>
    <tr>
        <td>2019-08</td>
        <td>Youtube</td>
        <td>614</td>
        <td>16</td>
        <td>2.61</td>
        <td>987</td>
        <td>950</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Facebook</td>
        <td>597</td>
        <td>27</td>
        <td>4.52</td>
        <td>2270</td>
        <td>2270</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Instagram</td>
        <td>588</td>
        <td>18</td>
        <td>3.06</td>
        <td>1260</td>
        <td>1122</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Organic</td>
        <td>592</td>
        <td>23</td>
        <td>3.89</td>
        <td>1267</td>
        <td>1267</td>
    </tr>
    <tr>
        <td>2019-09</td>
        <td>Youtube</td>
        <td>588</td>
        <td>15</td>
        <td>2.55</td>
        <td>1301</td>
        <td>1301</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Google</td>
        <td>699</td>
        <td>29</td>
        <td>4.15</td>
        <td>2150</td>
        <td>2098</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>Naver</td>
        <td>713</td>
        <td>32</td>
        <td>4.49</td>
        <td>2695</td>
        <td>2695</td>
    </tr>
    <tr>
        <td>2019-10</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Google</td>
        <td>688</td>
        <td>25</td>
        <td>3.63</td>
        <td>2286</td>
        <td>2235</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>Naver</td>
        <td>667</td>
        <td>23</td>
        <td>3.45</td>
        <td>2234</td>
        <td>1987</td>
    </tr>
    <tr>
        <td>2019-11</td>
        <td>TIKTOK</td>
        <td>0</td>
        <td>0</td>
        <td>0.00</td>
        <td>0</td>
        <td>0</td>
    </tr>
</table>


