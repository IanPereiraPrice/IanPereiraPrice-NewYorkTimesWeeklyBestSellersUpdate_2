import os

from functools import wraps

import pandas as pd

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect
import requests
import time
import smtplib
import json
from email.message import EmailMessage

args = {"owner": "Airflow", "start_date": days_ago(1)}

dag = DAG(dag_id="nyt_new_mail_etl_dag", default_args=args, schedule_interval=None)


def logger(func):
    from datetime import datetime, timezone

    @wraps(func)
    def wrapper(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return wrapper

@logger

def db_connection():
    print("testing db connection")
    drivername='postgresql+psycopg2'
    db_name='nyt_update'
    user='postgres'
    passwd='postgres'
    host='database'
    port=5432

    CONNECTION_STRING = f'{drivername}://{user}:{passwd}@{host}:{port}/{db_name}?client_encoding=utf8'
    
    engine = create_engine(CONNECTION_STRING, pool_pre_ping=True)
    engine.connect()
    return engine

@logger

def send_mail(df,email_list = []):
    for address in email_list:
        msg = EmailMessage()
        msg["to"] = address
        msg["from"] = "nytweeklyupdatebooklist@gmail.com"
        msg["Subject"] = "weekly update"
        text = ' '.join(df['title'].tolist()).capitalize()
        msg.set_content(text)


        #Create Smtp client, login to gmail and send the email
        with smtplib.SMTP_SSL("smtp.gmail.com","465") as smtp : 
            smtp.login("nytweeklyupdatebooklist@gmail.com","tdscdefwxmfgkhir")
            smtp.send_message(msg)
            print("message sent")


@logger
def send_request(date,list):
    #Sends a request to the NYT book archive for current desired list
    
    base_url = f'https://api.nytimes.com/svc/books/v3/lists.json?list-name={list}&api-key='
    nyt_books_api_key = 'cc9U6urInv2QBQYHfQ4GxnZvA5NzkBwe'
    url = base_url + nyt_books_api_key
    print(url)
    response = requests.get(url).json()
    #print(response)
    #time.sleep(6)
    return response
    
@logger
def parse_response(response):
    #return DF
    data = {'weeks_on_list': [],  
        'description': [],
         'title':[],
            'time':[],
           'rank':[]}
    
    time = response['last_modified']
    print(time)
    book_list = response['results'] 
    for book in book_list: # For each article, make sure it falls within our date range
        data['weeks_on_list'].append(book['weeks_on_list'])
        print(book['weeks_on_list'])
        data['description'].append(book['book_details'][0]['description']) 
        data['title'].append(book['book_details'][0]['title'])
        data['time'].append(time)
        data['rank'].append(book['rank'])
        print(book['book_details'][0]['title'])
    return pd.DataFrame(data) 
   
    
    
@logger
def check_table_exists(table_name, engine):
    if table_name in inspect(engine).get_table_names():
        print(f"{table_name!r} exists in the DB!")
    else:
        print(f"{table_name} does not exist in the DB!")
        
@logger
def mail_check(email_list = []):
    engine = db_connection()
    mailing_list = {'mailing_list':email_list}
    df_m = pd.DataFrame(mailing_list)
    
    
    if engine.has_table('mailing_list') == False:
        df_m.to_sql('nyt_mailing_list', engine,if_exists='replace')
        return df_m
    else:
        temp = pd.read_sql(f"SELECT * FROM {'nyt_mailing_list'}", engine)
        df2 = pd.merge(df_m, temp, on=['mailing_list'], how='left', indicator='Exist')
        df3 = pd.merge(df2, temp, on=['mailing_list'], how='left', indicator='Exist')
        df3['Exist'] = np.where(df.Exist == 'both', True, False)
        df_new = df3[['Exist']==False]
    
    
        if len(df_new.index) > 0:
            df_new.to_sql('nyt_mailing_list', engine, if_exists='append')
            return df_new
        else: pass
        
        
        
@logger
def nyt_call(table_name,api_call_name,email_list = []):
    engine = db_connection()
    response = send_request('current',api_call_name)
    df = parse_response(response)
    if email_list is not None:
     
        send_mail(df,email_list)

@logger
def tables_exists():
    db_engine = db_connection()
    
    check_table_exists("combined_print_and_e_book_fiction", db_engine)
    check_table_exists("combined_print_and_e_book_nonfiction", db_engine)
    db_engine.dispose()

@logger
def etl():
    email_list = ["chargersfan102@gmail.com","ianprice17@yahoo.com"]
    non_fic_list = 'combined-print-and-e-book-nonfiction'
    non_fiction = 'combined_print_and_e_book_nonfiction'
    fic_list = 'combined-print-and-e-book-fiction'
    fic = 'combined_print_and_e_book_fiction'
    engine=db_connection()
    df_mail = mail_check(email_list)
    mail_list = df_mail['mailing_list'].to_list()
    nyt_call(non_fiction,non_fic_list,mail_list)
    nyt_call(fic,fic_list,mail_list)
    engine.dispose

with dag:
    run_etl_task = PythonOperator(task_id="run_etl_task", python_callable=etl)
    run_tables_exists_task = PythonOperator(
        task_id="run_tables_exists_task", python_callable=tables_exists)

    run_etl_task >> run_tables_exists_task
