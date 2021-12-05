from datetime import datetime, date, timedelta
import json
from utils.general_utils import *
import pickle
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from sqlalchemy import create_engine
import requests

load_dotenv()

# TODO: Move to a table in redshift
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


def load_data_from_s3(folder_name, days_back=1):
    load_data_s3(folder_name, days_back=days_back)


def load_and_aggregate_emails(days_back=1):
    days_back = int(days_back)
    load_data_s3("Data_Science/Text_Data/Salesforce/EmailMessage/", days_back=days_back)
    emails = pd.read_csv('/valohai/outputs/loaded_source.csv')
    case_to_account_df = pd.read_csv('/valohai/inputs/case_to_account/case_to_account.csv', delimiter=";")
    payload = []

    case_to_account = {}
    for index, row in case_to_account_df.iterrows():
        case_to_account[row['id']] = [row['accountid'], row['name'], row['createddate']]

    cols = list(emails.columns)
    non_text_cols = ['Id', 'ParentId', 'Incoming', 'CreatedDate']
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in emails.iterrows():
        email_id = row['Id']
        case_id = row['ParentId'][:-3]
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
                    if not pd.isna(row[field]):
                        if term.lower() in row[field].lower():
                            temp_dict['account_id'] = account_id
                            temp_dict['instance_id'] = email_id
                            temp_dict['instance_date'] = row['CreatedDate']
                            temp_dict['term'] = sublist[0]
                            temp_dict['type'] = 'email_' + field + '_' + incoming
                # If temp dict is not empty than append to the final payload
                if temp_dict:
                    payload.append(temp_dict)

    print(len(payload))
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


def aggregate(source, days_back=1):
    payload = []
    source_df = None
    non_text_cols = None
    case_to_account = {}
    if source == 'tasks':
        source_df = pd.read_csv('/valohai/inputs/tasks/loaded_source.csv')
        non_text_cols = ['Id', 'AccountId', 'CreatedDate']
    elif source == 'sessions':
        source_df = pd.read_csv('/valohai/inputs/sessions/loaded_source.csv')
        non_text_cols = ['Id', 'Account__c', 'Session_Date__c', 'RecordTypeId']
    elif source == 'emails':
        days_back = int(days_back)
        load_data_s3("Data_Science/Text_Data/Salesforce/EmailMessage/", days_back=days_back)
        source_df = pd.read_csv('/valohai/outputs/loaded_source.csv')
        case_to_account_df = pd.read_csv('/valohai/inputs/case_to_account/case_to_account.csv', delimiter=";")

        for index, row in case_to_account_df.iterrows():
            case_to_account[row['id']] = [row['accountid'], row['name'], row['createddate']]

        non_text_cols = ['Id', 'ParentId', 'Incoming', 'CreatedDate']

    cols = list(source_df.columns)
    fields = [x for x in cols if x not in non_text_cols]
    for index, row in source_df.iterrows():
        account_id = None
        email_id = None
        if source == 'emails':
            email_id = row['Id']
            case_id = row['ParentId'][:-3]
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
                            if source == 'tasks':
                                temp_dict['account_id'] = row['AccountId']
                                temp_dict['instance_id'] = row['Id']
                                temp_dict['instance_date'] = row['CreatedDate']
                                temp_dict['term'] = sublist[0]
                                temp_dict['type'] = 'task_' + field
                            elif source == 'sessions':
                                temp_dict['account_id'] = row['Account__c']
                                temp_dict['instance_id'] = row['Id']
                                temp_dict['instance_date'] = row['Session_Date__c']
                                temp_dict['term'] = sublist[0]
                                temp_dict['type'] = 'session_' + field
                            elif source == 'emails':
                                temp_dict['account_id'] = account_id
                                temp_dict['instance_id'] = email_id
                                temp_dict['instance_date'] = row['CreatedDate']
                                temp_dict['term'] = sublist[0]
                                temp_dict['type'] = 'email_' + field + '_' + incoming

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


def load_data_from_redshift(sql_file_name):
    load_data_redshift(sql_file_name)


def upload_to_redshift(table_name, append="0"):
    append = bool(int(append))
    dbname = os.getenv('dbname')
    host = os.getenv('host')
    port = os.getenv('port')
    user = os.getenv('user')
    password = os.getenv('password')
    conn = create_engine(
        'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)

    final_df = pd.read_csv('/valohai/inputs/final/final.csv')
    final_df['insert_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_df['insert_date'] = datetime.now().strftime("%Y-%m-%d")
    final_df = final_df.loc[(final_df['instance_id'] != '02s6900002S2ieRAAR') & (final_df['account_id'] != '') & ~(
        final_df['account_id'].isna()), :]
    cols_to_trim = ['account_id', 'instance_id', 'instance_date', 'term', 'type']
    for col in list(final_df.columns):
        if col in cols_to_trim:
            print('max length of col ' + col)
            print(final_df[col].str.len().max())
            final_df[col] = final_df[col].apply(lambda x: str(x)[:255])

    total_rows = final_df.shape[0]
    first_insert = True
    iter = 1
    while final_df.shape[0] > 0:
        if_exists = 'append' if append or not first_insert else 'replace'
        chunk = final_df.tail(100000)
        chunk.to_sql(table_name,
                     conn,
                     schema='data_science',
                     index=False,
                     if_exists=if_exists,
                     chunksize=10000,
                     method='multi')
        print(chunk.shape[0])
        final_df = final_df.head(final_df.shape[0] - 100000)
        first_insert = False
        iter += 1

    message = "Text Intent Project: The table " + table_name + " got updated with " + str(total_rows) + " rows!"
    send_slack_message(message)
    print(message)