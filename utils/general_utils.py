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


def load_data_s3(source_folder, yesterday=False):
    AWS_KEY = os.getenv('AWS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_REGION = os.getenv('AWS_REGION')
    OUTPUT_PATH = os.getenv('VH_OUTPUTS_DIR')

    session = Session(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    s3 = session.resource('s3')
    your_bucket = s3.Bucket('prod-is-data-science-bucket')

    # file_name_for_save = folder_name.split('/')[-3]
    total_df = None
    if yesterday:
        yesterday = datetime.utcnow().date() - timedelta(days=2)
        year = str(yesterday.year)
        month = str(yesterday.month)
        day = str(yesterday.day)
        full_folder = source_folder + year + '/' + month + '/' + day
        object_name = None
        # print(full_folder)
        for file in your_bucket.objects.all():
            if full_folder in file.key:
                object_name = file.key

        if not object_name:
            print("Folder for the date " + str(yesterday) + " does not exist!")
        else:
            object = your_bucket.Object(object_name)
            object.download_file(OUTPUT_PATH + '/loaded_source.csv')
            total_df = pd.read_csv(OUTPUT_PATH + '/loaded_source.csv', error_bad_lines=False, sep='\001')

    else:
        # Since the data in email messages is too big, we skip older years for that case
        only_later_years = 'EmailMessage' in source_folder

        if 'EmailMessage' in source_folder:
            df_columns = ['Id', 'ParentId', 'Subject', 'TextBody', 'Incoming', 'CreatedDate']
        elif 'TechnicalInfo' in source_folder:
            df_columns = ['Id', 'Account__c', 'Case__c', 'Session_Date__c', 'Background__c',
                          'Sales_Notes__c', 'Goal_of_the_meeting__c', 'Comments_of_client__c',
                          'Architecture__c', 'Internal_Notes__c', 'External_Notes__c', 'RecordTypeId']
        else:
            df_columns = ['Id', 'AccountId', 'Comments__c', 'Subject', 'CreatedDate']

        object_list = []
        for file in your_bucket.objects.all():
            if source_folder in file.key:
                object_name = file.key
                object_list.append(object_name)

        first_obj = True
        for obj in object_list:
            if only_later_years:
                year = int(str(obj).split('/')[4])
                if year < 2021:
                    continue

            curr_obj = your_bucket.Object(obj)
            # print(obj)
            curr_obj.download_file(OUTPUT_PATH + '/curr_sheet.csv')

            curr_df = pd.read_csv(OUTPUT_PATH + '/curr_sheet.csv',
                                  delimiter='\x01',
                                  lineterminator='\x04',
                                  usecols=df_columns,
                                  encoding='utf-8',
                                  skip_blank_lines=True)
            # print(curr_df.shape)
            # print(curr_df.columns)
            curr_df.to_csv(OUTPUT_PATH + '/curr_sheet.csv', index=False)
            os.remove(OUTPUT_PATH + '/curr_sheet.csv')
            if first_obj:
                total_df = curr_df
            else:
                total_df = pd.concat([total_df, curr_df])
            first_obj = False

    total_df.to_csv(OUTPUT_PATH + '/loaded_source.csv', index=False)
    return total_df


def load_data_s3_with_date_filter(folder_name, date_from):
    AWS_KEY = os.getenv('AWS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_REGION = os.getenv('AWS_REGION')
    session = Session(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    s3 = session.resource('s3')
    your_bucket = s3.Bucket('prod-is-data-science-bucket')

    file_name_for_save = folder_name.split('/')[-3]
    object_name = ""
    for file in your_bucket.objects.all():
        if folder_name in file.key:
            object_name = file.key

    object = your_bucket.Object(object_name)
    OUTPUT_PATH = os.getenv('VH_OUTPUTS_DIR')
    object.download_file(OUTPUT_PATH + '/' + file_name_for_save + '.csv')
    df = pd.read_csv(OUTPUT_PATH + '/' + file_name_for_save + '.csv', error_bad_lines=False)
    print(date_from)
    if 'CreatedDate' in list(df.columns):
        df['CreatedDate'] = pd.to_datetime(df['CreatedDate'])
        df = df[df['CreatedDate'] >= date_from]
    else:
        df['Session_Date__c'] = pd.to_datetime(df['Session_Date__c'])
        df = df[df['Session_Date__c'] >= date_from]

    df.to_csv(OUTPUT_PATH + '/' + file_name_for_save + '.csv', index=False)
    return df


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


# - not used in this project
def get_cat_features(x):
    """
    :param x: a Data Frame
    :return: indices of categorical columns
    """
    return np.where((x.dtypes != np.float) & (x.dtypes != np.int))[0]


# - returns the names of the categorical features in the input (used for Catboost)
def get_cat_feature_names(X):
    """

    :param X: the input features (a DataFrame)
    :return: the names of the categorical features
    """
    return [col for col in X.columns if X.dtypes[col] not in [np.int, np.float]]


# - get the technologies names
def get_technologies():
    return ['maven', 'generic', 'buildinfo', 'docker', 'npm', 'pypi', 'gradle', 'nuget', 'yum',
            'helm', 'gems', 'debian', 'ivy', 'sbt', 'conan', 'bower', 'go', 'chef', 'gitlfs',
            'composer', 'puppet', 'conda', 'vagrant', 'cocoapods', 'cran', 'opkg', 'p2', 'vcs', 'alpine']


# - load the trials data, and the training leads data with a pickle.  If it was loaded from the db lately than load the
# corresponding pickle, otherwise load it from the db using load_data method. In the one hand we will not be up-to-date
# in each time we work with the training data, but on the other hand we will not load the queries each time

def load_train_trials_pickle(days_between_loads=7):
    """

    :param days_between_loads: how frequently we would like to load the data from the db
    (instead of the pickle which is not up-to-date)
    :return: the leads data of the training set  (labeled data), and the trials data
    """
    last_load_time = pickle.load(open(r'pickle/last_load_time.pkl', 'rb'))
    if (date.today() - last_load_time).days >= days_between_loads:
        df_train = load_data('get_data_train.sql')
        df_trials = load_data('get_trials.sql')
        pickle.dump(df_train, open(r'pickle/df_train.pkl', 'wb'))
        pickle.dump(df_trials, open(r'pickle/df_trails.pkl', 'wb'))
        last_load_time = date.today()
        pickle.dump(last_load_time, open(r'pickle/last_load_time.pkl', 'wb'))
    else:
        df_train = pickle.load(open(r'pickle/df_train.pkl', 'rb'))
        df_trials = pickle.load(open(r'pickle/df_trails.pkl', 'rb'))
    return df_train, df_trials
