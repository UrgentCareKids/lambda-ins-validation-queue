from numpy import insert
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
import psycopg2
import json
import pandas.io.sql as sqlio
pd.set_option('display.max_columns', 500)
from datetime import datetime, timedelta, date
import boto3
import os


ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
param = ssm.get_parameter(Name='uck-etl-db-prod-masterdata', WithDecryption=True )
db_request = json.loads(param['Parameter']['Value']) 

ssm_insval = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
param_insval = ssm_insval.get_parameter(Name='uck-etl-db-ins-val-svc-dev', WithDecryption=True )
db_request_insval = json.loads(param_insval['Parameter']['Value']) 

ssm_redshift = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
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

def redshift_conn():
    hostname = db_request_redshift['host']
    portno = db_request_redshift['port']
    dbname = db_request_redshift['database']
    dbusername = db_request_redshift['user']
    dbpassword = db_request_redshift['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    return conn

def get_patient():
    _targetconnection = insval_conn()
    cur = _targetconnection.cursor()
    select_query = f'select queue_id, patient_id from public.insval_queue where task_available = true'
    cur.execute(select_query,)
    patients = cur.fetchall()
    df = pd.DataFrame(patients)
    for i in range(len(df)): 
        print(df.iloc[i,0], df.iloc[i,1])
        queue_id = df.iloc[i,0]
        patient_id = df.iloc[i,1]
        get_patient_details(queue_id, patient_id)

def get_patient_details(queue_id, patient_id):
    _targetconnection = masterdata_conn()
    cur = _targetconnection.cursor()
    select_query = f"select primary_ins_id from mat_tmp_fast_demographics where pond_id = '{patient_id}'"
    cur.execute(select_query,)
    ins_id = cur.fetchall()
    df = pd.DataFrame(ins_id)
    for i in range(len(df)): 
        print(df.iloc[i,0])
        ins_id = df.iloc[i,0]
        map_ins(queue_id,ins_id)


def map_ins(queue_id, ins_id):
    _targetconnection = redshift_conn()
    cur = _targetconnection.cursor()
    select_query = f"select ext_id from map_srv.ins_cx where pri_ins_id ilike '{ins_id}' and ext_source = 'WAVE'"
    cur.execute(select_query,)
    ext_id = cur.fetchall()
    df = pd.DataFrame(ext_id)
    for i in range(len(df)):
        if df.iloc[i,0] == None or '': 
            request_type = 'DISCO'
        else:
            payer_code = df.iloc[i,0]
            request_type = 'ELIG'
    print('payer_code: ',payer_code)
    insert_into_insval(queue_id,payer_code, request_type)

def insert_into_insval(queue_id, payer_code, request_type):
    _targetconnection = insval_conn()
    cur = _targetconnection.cursor()
    update_query = f"update public.insval_queue set payer_code = '{payer_code}', request_type = '{request_type}', task_available = false where queue_id = '{queue_id}'"
    cur.execute(update_query,)
    _targetconnection.commit()
    print('Done', queue_id, payer_code)


get_patient()
    
# def employee_list(schema,company):
#     _targetconnection = redshift_conn()
#     sql = 'select * from '+schema+'.paylocity_employee where company_id='+company;
#     employees_df = sqlio.read_sql_query(sql, _targetconnection)
#     employee_company_dict = dict(zip(employees_df.employee_id , employees_df.company_id))
#     return employee_company_dict

# def refresh_table(tbl_nm):
#     _targetconnection = redshift_conn()
#     cur = _targetconnection.cursor()
#     cur.execute(f"call {schema}.refresh_{tbl_nm}();")
#     _targetconnection.commit()

# def clear_table(tbl_nm):
#     _targetconnection = redshift_conn()
#     cur = _targetconnection.cursor()
#     cur.execute(f"delete from {schema}.{tbl_nm};")
#     _targetconnection.commit()

# def log(message, status):
#     _targetconnection = redshift_conn()
#     cur = _targetconnection.cursor()
#     cur.execute(f"insert into {schema}.paylocity_log(message, status) values('{message}', '{status}');")
#     _targetconnection.commit()


# def rate_counter():
#     ...



# # json loads

# def paylocity_company_codes_json():
#     clear_table('paylocity_company_codes_json')
#     for compval in company_id:       
#         token = token_autentication()
#         headers=  {'Authorization': 'Bearer ' + str(token['access_token'])}
#         company_location_request = requests.get('https://api.paylocity.com/api/v2/companies/'+compval+'/codes/costCenter1',headers=headers)
#         company_location_response = company_location_request.json()
#         insert_into_redshift('paylocity_company_codes_json',company_location_response, compval)

# #
# def paylocity_employee_json():
#     clear_table('paylocity_employee_json')
#     token = token_autentication()
#     for company in company_id:
#         headers=  {'Authorization': 'Bearer ' + str(token['access_token'])}
#         headers_request = requests.get('https://api.paylocity.com/api/v2/companies/'+company+'/employees', headers=headers)
#         response_headers = headers_request.headers
#         page_size = response_headers['X-Pcty-Total-Count']
#         employees_request = requests.get('https://api.paylocity.com/api/v2/companies/'+company+'/employees?pagesize='+page_size, headers=headers)
#         employees_response = employees_request.json()
#         insert_into_redshift('paylocity_employee_json',employees_response, company)

# def paylocity_employee_details_json():
#     clear_table('paylocity_employee_details_json')
#     for company in company_id:
#         employee_company_dict = employee_list(schema,company)
#         token = token_autentication()
#         for employee_id,compval in employee_company_dict.items():
#             print('loading ' + employee_id)
#             headers=  {'Authorization': 'Bearer ' + str(token['access_token'])}
#             employee_details_request = requests.get('https://api.paylocity.com/api/v2/companies/' +compval + '/employees/' +employee_id, headers=headers)
#             employee_details_response = employee_details_request.json()
#             insert_into_redshift('paylocity_employee_details_json',employee_details_response, compval, employee_id)

# #
# def paylocity_employee_paystatement_json():
#     clear_table('paylocity_employee_paystatement_json')
#     for company in company_id:
#         employee_company_dict = employee_list(schema,company)
#         token = token_autentication()
#         for employee_id,val in employee_company_dict.items():
#                 headers=  {'Authorization': 'Bearer ' + str(token['access_token'])}
#                 headers_request = requests.get('https://api.paylocity.com/api/v2/companies/'+val+'/employees/'+employee_id+'/paystatement/summary/'+year, headers=headers)
#                 response_headers = headers_request.headers
#                 page_size = response_headers['x-pcty-total-count']
#                 if(int(page_size) > 0):                
#                     print('loading ' + employee_id)
#                     pay_summary_request = requests.get('https://api.paylocity.com/api/v2/companies/'+val+'/employees/'+employee_id+'/paystatement/summary/'+str(year)+'?pagesize='+page_size, headers=headers)
#                     pay_summary_response = pay_summary_request.json()
#                     insert_into_redshift('paylocity_employee_paystatement_json',pay_summary_response, company, employee_id)


# # get_employee_details(schema)

# def load_json_tables():
#     try:
#         log('process kicked off', 'info')
#         #phase 1 json load
#         paylocity_company_codes_json()
#         log('paylocity_company_codes_json - completed', 'info')
#         paylocity_employee_json()
#         log('paylocity_employee_json - completed', 'info')
#         #parsers phase1:
#         refresh_table('paylocity_company_codes')
#         log('refresh paylocity_company_codes - completed', 'info')
#         refresh_table('paylocity_employee')
#         log('refresh paylocity_employee - completed', 'info')

#         paylocity_employee_paystatement_json()
#         log('paylocity_employee_paystatement_json - completed', 'info')
#         paylocity_employee_details_json()
#         log('paylocity_employee_details_json - completed', 'info')
#         refresh_table('paylocity_employee_paystatement')
#         log('refresh paylocity_employee_paystatement - completed', 'info')
#         refresh_table('paylocity_employee_details')
#         log('refresh paylocity_employee_details - completed', 'info')
#         log('process completed - success', 'info')
#     except Exception as e:
#         print(e)
#         log(e, 'error')
#     #these cant' run until the first 1 (employee) is refreshed

# load_json_tables()