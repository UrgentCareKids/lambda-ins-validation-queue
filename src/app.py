import pandas as pd
import psycopg2
import json
import pandas.io.sql as sqlio
pd.set_option('display.max_columns', 500)
import boto3
import os
from loguru import logger

def handler(event,context):
    body = event['Records'][0]['Sns']['Message']
    logger.info('got here', event)
    queue_id = body['queue_id']
    patient_id = body['patient_id']
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
    select_query = f"select mtfd.primary_ins_id, ic.ext_id from mat_tmp_fast_demographics mtfd left join public.ins_cx ic on mtfd.primary_ins_id::bigint = ic.pri_ins_id where mtfd.pond_id = '{patient_id}' and mtfd.primary_ins_id::bigint = ic.pri_ins_id  and ic.ext_source = 'WAVE'"
    cur.execute(select_query,)
    ins_id = cur.fetchall()
    df = pd.DataFrame(ins_id)
    payer_code = ''
    request_type = ''
    if df.empty == True:
        request_type = 'DISCO'
    else: 
        for i in range(len(df)): 
            ins_id = df.iloc[i,0]
            payer_code = df.iloc[i,1]
            request_type = 'ELIG'
    _targetconnection = insval_conn()
    cur = _targetconnection.cursor()
    print(request_type, payer_code)
    update_query = f"update public.insval_queue set payer_code = '{payer_code}', request_type = '{request_type}' where queue_id = '{queue_id}'"
    cur.execute(update_query,)
    _targetconnection.commit()
    print('DONE', request_type, patient_id)


