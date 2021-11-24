from datetime import datetime, date, timedelta
import os
import shap
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, StackingClassifier, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from utils.fe_utils import *
from sklearn.metrics import precision_recall_curve, auc
from utils.general_utils import *
import pickle
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from sqlalchemy import create_engine
import requests

load_dotenv()

trigger_terms = [['high availability', 'high-availability', ' ha ', ' ha.', ' ha,'], ['enterprise'],
                 ['multiple', 'multi site', 'multi-site',
                  'multisite', 'multiple sites'], ['downtime', 'down time'], ['bad performance', 'low performance'],
                 ['replications', 'replicate', 'replication'], ['balance', 'balancing',
                                                                'balancer'], ['disaster recovery', ' dr ',
                                                                              'business continuity'],
                 ['permission handle',
                  'permissions handle', 'permission handling', 'permissions handling'],
                 ['distribution', 'docker', 'generic', 'yum'], ['Storage Sharding', 'Storage-Sharding', 'Sharding'],
                 ['Xray'], ['Scanning', 'vulnerability', 'JXray'], ['CVE', 'VulnDB', 'Nvd', 'Xuc'],
                 ['compliance', 'Security', 'Ciso', 'Devsecops'],
                 ['Cyber'], ['Budget', 'attrition', 'budgeting', 'paying', '$', 'expansive'],
                 ['SLA', 'frustration', 'complains', 'complaint', 'Negative', 'risk'],
                 ['downsell', 'downsale', 'downgrade', 'down-sell', 'down sell'], ['churn'], ['support'],
                 ['Competitor', 'Alcide', 'Anchore', 'Aqua Security', 'aquasecurity', 'Black duck', 'blackduck',
                  'Black Duck', 'Synopsys', 'CAST Software', 'castsoftware', 'CodeReady Dependency Analytics',
                  'Contrast Security', 'CxSCA', 'Deep Security Smart Check', 'Dependency-Check (OWASP)',
                  'Dependency-Track (OWASP)',
                  'Fortify', 'FOSSA', 'Kiuwan', 'NeuVector', 'Nexus IQ', 'Open Source Guardian',
                  'Prisma Cloud (formerly Twistlock)',
                  'Qualys', 'ShiftLeft Scan', 'Snyk', 'Tenable', 'Trivy', 'Veracode', 'whitehat', 'White Hat Security',
                  'WhiteSource']]


def send_slack_message(message):
    now_str = str(datetime.today())
    dict_for_post = {'Message': message,
                     'Created_Date': now_str}
    try:
        requests.post(url="https://www.workato.com/webhooks/rest/90076a68-0ba3-4091-aa8d-9da27893cfd6/test",
                      data=json.dumps(dict_for_post))
        print("Successfully sent update to the data-science slack channel.")
    except:
        print("Failed to sent message to data-science slack channel. please check the Workato recipe related to this.")


def load_data_from_s3(folder_name, yesterday=0):
    yesterday = yesterday == 1
    load_data_s3(folder_name, yesterday=yesterday)


# def load_data_from_s3_all_subfolders(source_folder):


def load_data_from_redshift(sql_file_name):
    load_data_redshift(sql_file_name)


def load_and_aggregate_emails():
    load_data_s3("Data_Science/Text_Data/Salesforce/EmailMessage/")
    emails = pd.read_csv('/valohai/outputs/loaded_source.csv')
    case_to_account_df = pd.read_csv('/valohai/inputs/case_to_account/case_to_account.csv', delimiter=";")
    payload = []

    print(list(case_to_account_df.columns))
    print(case_to_account_df.shape)
    print(list(emails.columns))
    print(emails.shape)


    case_to_account = {}
    for index, row in case_to_account_df.iterrows():
        case_to_account[row['id']] = [row['accountid'], row['name'], row['createddate']]

    cols = list(emails.columns)
    non_text_cols = ['Id', 'ParentId', 'Incoming', 'CreatedDate']
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in emails.iterrows():
        print('iter')
        email_id = row['Id']
        case_id = row['ParentId']
        if case_id not in case_to_account:
            continue

        account_id = case_to_account[case_id][0]

        if str(account_id) == 'nan':
            continue

        incoming = "incoming" if row['Incoming'] == 'true' else 'outgoing'
        for field in fields:
            for sublist in trigger_terms:
                temp_dict = {}
                for term in sublist:
                    if not pd.isnull(row[field]):
                        if term.lower() in row[field].lower():
                            temp_dict['account_id'] = account_id
                            temp_dict['instance_id'] = email_id
                            temp_dict['instance_date'] = row['CreatedDate']
                            temp_dict['term'] = sublist[0]
                            temp_dict['type'] = 'email_' + field + '_' + incoming
                # If temp dict is not empty than append to the final payload
                if temp_dict:
                    payload.append(temp_dict)

    final_df = pd.DataFrame(payload)
    final_df.to_csv('/valohai/outputs/emails.csv', index=False)


def aggregate_emails():
    case_to_account_df = pd.read_csv('/valohai/inputs/case_to_account/case_to_account.csv', delimiter=";")
    emails = pd.read_csv('/valohai/inputs/emails/EmailMessage.csv')
    payload = []

    cols = list(case_to_account_df.columns)
    print(cols)
    cols = list(emails.columns)
    print(cols)

    case_to_account = {}
    for index, row in case_to_account_df.iterrows():
        case_to_account[row['id']] = [row['accountid'], row['name'], row['createddate']]

    cols = list(emails.columns)
    non_text_cols = ['Id', 'ParentId', 'Incoming', 'CreatedDate']
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in emails.iterrows():
        email_id = row['Id']
        case_id = row['ParentId']
        account_id = case_to_account[case_id][0]
        if case_id not in case_to_account or str(account_id) == 'nan':
            continue

        incoming = "incoming" if row['Incoming'] == 'true' else 'outgoing'
        for field in fields:
            for sublist in trigger_terms:
                temp_dict = {}
                for term in sublist:
                    if not pd.isnull(row[field]):
                        if term.lower() in row[field].lower():
                            temp_dict['account_id'] = account_id
                            temp_dict['instance_id'] = email_id
                            temp_dict['instance_date'] = row['CreatedDate']
                            temp_dict['term'] = sublist[0]
                            temp_dict['type'] = 'email_' + field + '_' + incoming
                # If temp dict is not empty than append to the final payload
                if temp_dict:
                    payload.append(temp_dict)

    final_df = pd.DataFrame(payload)
    final_df.to_csv('/valohai/outputs/emails.csv', index=False)


def aggregate_sessions():
    sessions = pd.read_csv('/valohai/inputs/sessions/loaded_source.csv')
    payload = []

    cols = list(sessions.columns)
    print(cols)
    non_text_cols = ['Id', 'Account__c', 'Session_Date__c', 'RecordTypeId']
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in sessions.iterrows():
        for field in fields:
            for sublist in trigger_terms:
                temp_dict = {}
                for term in sublist:
                    if not pd.isnull(row[field]):
                        if term.lower() in row[field].lower():
                            temp_dict['account_id'] = row['Account__c']
                            temp_dict['instance_id'] = row['Id']
                            temp_dict['instance_date'] = row['Session_Date__c']
                            temp_dict['term'] = sublist[0]
                            temp_dict['type'] = 'session_' + field
                # If temp dict is not empty than append to the final payload
                if temp_dict:
                    payload.append(temp_dict)

    final_df = pd.DataFrame(payload)
    final_df.to_csv('/valohai/outputs/sessions.csv', index=False)


def aggregate_tasks():
    tasks = pd.read_csv('/valohai/inputs/tasks/loaded_source.csv')
    payload = []

    cols = list(tasks.columns)
    print(cols)
    non_text_cols = ['Id', 'AccountId', 'CreatedDate']
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in tasks.iterrows():
        for field in fields:
            for sublist in trigger_terms:
                temp_dict = {}
                for term in sublist:

                    if not pd.isnull(row[field]):
                        if term in row[field]:
                            temp_dict['account_id'] = row['AccountId']
                            temp_dict['instance_id'] = row['Id']
                            temp_dict['instance_date'] = row['CreatedDate']
                            temp_dict['term'] = sublist[0]
                            temp_dict['type'] = 'task_' + field
                # If temp dict is not empty than append to the final payload
                if temp_dict:
                    payload.append(temp_dict)

    final_df = pd.DataFrame(payload)
    final_df.to_csv('/valohai/outputs/tasks.csv', index=False)


def concat_all():
    files_list = []
    try:
        emails = pd.read_csv('/valohai/inputs/emails/emails.csv')
        files_list.append(emails)
    except pd.errors.EmptyDataError:
        print("emails file" + " is empty and has been skipped.")

    try:
        sessions = pd.read_csv('/valohai/inputs/sessions/sessions.csv')
        files_list.append(sessions)
    except pd.errors.EmptyDataError:
        print("sessions file" + " is empty and has been skipped.")

    try:
        tasks = pd.read_csv('/valohai/inputs/tasks/tasks.csv')
        files_list.append(tasks)
    except pd.errors.EmptyDataError:
        print("tasks file" + " is empty and has been skipped.")

    if len(files_list) > 0:
        all_data = pd.concat(files_list)
        all_data.to_csv('/valohai/outputs/final.csv', index=False)


def upload_to_redshift(table_name):
    dbname = os.getenv('dbname')
    host = os.getenv('host')
    port = os.getenv('port')
    user = os.getenv('user')
    password = os.getenv('password')
    final_df = pd.read_csv('/valohai/inputs/final/final.csv')
    conn = create_engine(
        'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)
    final_df.to_sql(table_name,
                    conn,
                    schema='data_science',
                    index=False,
                    if_exists='replace',
                    chunksize=10000,
                    method='multi')
    message = "Text Intent Project: The table " + table_name + " got updated with " + str(final_df.shape[0]) + " rows!"
    send_slack_message(message)
    print(message)


def upload_to_s3(folder_path, file_name):
    df_with_predictions = pd.read_csv('/valohai/inputs/data_with_predictions/data_with_predictions.csv')
    filename = 'final_prediction.csv'
    df_with_predictions.to_csv('/valohai/outputs/' + filename)
    AWS_KEY = os.getenv('AWS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET')
    AWS_BUCKET = boto.connect_s3(AWS_KEY, AWS_SECRET).get_bucket('prod-is-data-science-bucket')
    s3_upload_folder_path = 'csat_model/valohai/upload/'
    local_path = '/valohai/outputs/' + filename
    key = Key(AWS_BUCKET, s3_upload_folder_path + filename)
    key.set_contents_from_filename(local_path)
