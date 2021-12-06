from boto3.session import Session
from boto.s3.key import Key
import boto
import psycopg2
import csv
import pandas as pd
import os
import numpy as np
import pickle
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

load_dotenv()


def explore_bucket_s3():
    AWS_KEY = os.getenv('AWS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_REGION = os.getenv('AWS_REGION')
    session = Session(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    s3 = session.resource('s3')
    your_bucket = s3.Bucket('prod-is-data-science-bucket')
    for file in your_bucket.objects.all():
        print(file)


def load_data_s3(source_folder, days_back=1):
    AWS_KEY = os.getenv('AWS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_REGION = os.getenv('AWS_REGION')
    OUTPUT_PATH = os.getenv('VH_OUTPUTS_DIR')
    session = Session(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    s3 = session.resource('s3')
    your_bucket = s3.Bucket('prod-is-data-science-bucket')

    if 'EmailMessage' in source_folder:
        df_columns = ['Id', 'ParentId',
                      'Subject', 'TextBody', 'Incoming', 'CreatedDate']
    elif 'TechnicalInfo' in source_folder:
        df_columns = ['Id', 'Account__c', 'Case__c', 'Session_Date__c', 'Background__c',
                      'Sales_Notes__c', 'Goal_of_the_meeting__c', 'Comments_of_client__c',
                      'Architecture__c', 'Internal_Notes__c', 'External_Notes__c', 'RecordTypeId']
    else:
        df_columns = ['Id', 'AccountId',
                      'Comments__c', 'Subject', 'CreatedDate']

    total_df = None
    since_date = datetime.utcnow().date() - timedelta(days=days_back)
    year = str(since_date.year)
    month = str(since_date.month)
    day = str(since_date.day)
    if days_back < 1:
        print("Minimum days_back parameter is 1! please change the code")
    else:
        object_list = []
        for file in your_bucket.objects.all():
            if source_folder in file.key:
                object_name = file.key
                object_list.append(object_name)

        first_obj = True
        for obj in object_list:

            year_str = int(str(obj).split('/')[4])
            month_str = int(str(obj).split('/')[5])
            day_str = int(str(obj).split('/')[6])
            if year_str < int(year):
                continue
            elif month_str < int(month):
                continue
            elif day_str < int(day):
                continue

            curr_obj = your_bucket.Object(obj)
            curr_obj.download_file(OUTPUT_PATH + '/curr_sheet.csv')
            curr_df = pd.read_csv(OUTPUT_PATH + '/curr_sheet.csv',
                                  delimiter='\x01',
                                  lineterminator='\x04',
                                  usecols=df_columns,
                                  encoding='utf-8',
                                  skip_blank_lines=True)
            curr_df.to_csv(OUTPUT_PATH + '/curr_sheet.csv', index=False)
            os.remove(OUTPUT_PATH + '/curr_sheet.csv')
            if first_obj:
                total_df = curr_df
            else:
                total_df = pd.concat([total_df, curr_df])
            first_obj = False

    total_df.to_csv(OUTPUT_PATH + '/loaded_source.csv', index=False)
    return total_df


# - load data from Red Shift
def load_data_redshift(query_name):
    """
    :param query_name: the name of the query (saved in sql file)
    :return: a data frame which is the output of the specified query
    """
    # idea_path = os.getenv('VH_REPOSITORY_DIR', os.getenv('path') + os.getcwd().rsplit('/', 1)[-1])
    idea_path = '/valohai/repository'
    query = open(idea_path + '/sql/' + query_name, 'r')
    con = psycopg2.connect(dbname=os.getenv('dbname'), host=os.getenv('host'),
                           port=os.getenv('port'), user=os.getenv('user'), password=os.getenv('password'))
    cur = con.cursor()
    q = query.read()
    cur.execute(q)
    rows = cur.fetchall()
    # path = open(idea_path + os.getcwd().rsplit('/', 1)[-1] + '/data/' + query_name[:-4] + '.csv', 'w')
    OUTPUT_PATH = os.getenv('VH_OUTPUTS_DIR', idea_path + os.getcwd().rsplit('/', 1)[-1])
    path = open(os.path.join(OUTPUT_PATH, query_name[:-4] + '.csv'), 'w')
    myFile = csv.writer(path, delimiter=';')
    myFile.writerow(col[0] for col in cur.description)
    myFile.writerows(rows)
    query.close()
    # query_result = pd.read_csv(path, delimiter=';', header=0)
    path.close()
    # return query_result


# - load data from Red Shift
def load_data_old(query_name):
    """
    :param query_name: the name of the query (saved in sql file)
    :return: a data frame which is the output of the specified query
    """
    idea_path = os.getenv('path')
    query = open(idea_path + os.getcwd().rsplit('/', 1)[-1] + '/sql/' + query_name, 'r')
    con = psycopg2.connect(dbname=os.getenv('dbname'), host=os.getenv('host'),
                           port=os.getenv('port'), user=os.getenv('user'), password=os.getenv('password'))
    cur = con.cursor()
    q = query.read()
    cur.execute(q)
    rows = cur.fetchall()
    path = open(idea_path + os.getcwd().rsplit('/', 1)[-1] + '/data/' + query_name[:-4] + '.csv', 'w')
    myFile = csv.writer(path, delimiter=';')
    myFile.writerow(col[0] for col in cur.description)
    myFile.writerows(rows)
    query.close()
    path.close()
    query_result = pd.read_csv(idea_path + os.getcwd().rsplit('/', 1)[-1] + '/data/' + query_name[:0 - 4] + '.csv',
                               delimiter=';', header=0)
    return query_result
