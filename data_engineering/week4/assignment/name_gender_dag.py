# coding=utf-8
import logging
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def get_redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "leemingyu05"  # 본인 ID 사용
    redshift_pass = "leemingyu05"  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("transform started")
    # ignore the first line - header
    lines = text.split("\n")[1:]
    logging.info("transform done")
    return lines


def load(lines):
    logging.info("load started")
    sql = "BEGIN; DELETE FROM leemingyu05.name_gender; INSERT INTO leemingyu05.name_gender VALUES "
    insert_sql = []

    for r in lines:
        if r == '':
            continue

        (name, gender) = r.split(",")
        print(name, "-", gender)
        insert_sql.append("('{name}', '{gender}')".format(name=name, gender=gender))

    sql = sql + ', '.join(insert_sql) + '; END;'

    cursor = get_redshift_connection()
    cursor.execute(sql)
    logging.info(sql)
    logging.info("load done")


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
    dag_id='second_assignment',
    start_date=datetime(2020, 8, 10),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='45 23 * * *')  # 적당히 조절

task = PythonOperator(
    task_id='perform_etl',
    python_callable=etl,
    dag=dag_second_assignment)
