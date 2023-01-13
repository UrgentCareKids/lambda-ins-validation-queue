import pandas as pd
import psycopg2
import json
import pandas.io.sql as sqlio
pd.set_option('display.max_columns', 500)
import boto3
import os
from loguru import logger

def handler(event,context):
    logger.info('got here', event)
    queue_id = event['queue_id']
    patient_id = event['patient_id']
    print(event)
    get_patient_details(queue_id, patient_id)

ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param = ssm.get_parameter(Name='uck-etl-db-prod-masterdata', WithDecryption=True )
db_request = json.loads(param['Parameter']['Value']) 

ssm_insval = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param_insval = ssm_insval.get_parameter(Name='uck-etl-db-ins-val-svc-dev', WithDecryption=True )
db_request_insval = json.loads(param_insval['Parameter']['Value']) 

ssm_redshift = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
param_redshift = ssm_redshift.get_parameter(Name='uck-etl-wave-hdc', WithDecryption=True )
db_request_redshift = json.loads(param_redshift['Parameter']['Value']) 

def masterdata_conn():
    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn

def insval_conn():
    hostname = db_request_insval['host']
    portno = db_request_insval['port']
    dbname = db_request_insval['database']
    dbusername = db_request_insval['user']
    dbpassword = db_request_insval['password']
    conn_insval = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn_insval


# def get_patient(queue_id, patient_id):
#     _targetconnection = insval_conn()
#     cur = _targetconnection.cursor()
#     select_query = f'select queue_id, patient_id from public.insval_queue where task_available = true'
#     cur.execute(select_query,)
#     patients = cur.fetchall()
#     df = pd.DataFrame(patients)
#     for i in range(len(df)): 
#         print(df.iloc[i,0], df.iloc[i,1])
#         queue_id = df.iloc[i,0]
#         patient_id = df.iloc[i,1]
#         get_patient_details(queue_id, patient_id)

def get_patient_details(queue_id, patient_id):
    _targetconnection = masterdata_conn()
    cur = _targetconnection.cursor()
    print('got here')
    select_query = f"select mtfd.primary_ins_id, ic.ext_id from mat_tmp_fast_demographics mtfd left join public.ins_cx ic on mtfd.primary_ins_id::bigint = ic.pri_ins_id where mtfd.pond_id = '{patient_id}' and mtfd.primary_ins_id::bigint = ic.pri_ins_id  and ic.ext_source = 'WAVE'"
    cur.execute(select_query,)
    ins_id = cur.fetchall()
    df = pd.DataFrame(ins_id)
    for i in range(len(df)): 
        print(df.iloc[i,0])
        ins_id = df.iloc[i,0]
        payer_code = df.iloc[i,1]
        if payer_code == None or '': 
            request_type = 'DISCO'
        else:
            request_type = 'ELIG'
        print(request_type)
        _targetconnection = insval_conn()
        cur = _targetconnection.cursor()
        print(request_type, payer_code)
        update_query = f"update public.insval_queue set payer_code = '{payer_code}', request_type = '{request_type}', where queue_id = '{queue_id}'"
        cur.execute(update_query,)
        _targetconnection.commit()
        print('Done', queue_id, payer_code)


# def map_ins(queue_id, ins_id):
#     _targetconnection = redshift_conn()
#     cur = _targetconnection.cursor()
#     select_query = f"select ext_id from map_srv.ins_cx where pri_ins_id ilike '{ins_id}' and ext_source = 'WAVE'"
#     cur.execute(select_query,)
#     ext_id = cur.fetchall()
#     df = pd.DataFrame(ext_id)
#     for i in range(len(df)):
#         if df.iloc[i,0] == None or '': 
#             request_type = 'DISCO'
#         else:
#             payer_code = df.iloc[i,0]
#             request_type = 'ELIG'
#     print('payer_code: ',payer_code)
#     insert_into_insval(queue_id,payer_code, request_type)

def insert_into_insval(queue_id, payer_code, request_type):
    _targetconnection = insval_conn()
    cur = _targetconnection.cursor()
    print(request_type, payer_code)
    update_query = f"update public.insval_queue set payer_code = '{payer_code}', request_type = '{request_type}', where queue_id = '{queue_id}'"
    cur.execute(update_query,)
    _targetconnection.commit()
    print('Done', queue_id, payer_code)

