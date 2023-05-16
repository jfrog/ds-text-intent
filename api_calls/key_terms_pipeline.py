import requests

resp = requests.request(
    url="https://valohai-prod-is.jfrog.org/api/v0/pipelines/",
    method="POST",
    headers={"Authorization": "Token YOUR_TOKEN_HERE"},
    json={
        "edges": [
            {
                "source_node": "load_case_to_account",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "load_and_aggregate_emails",
                "target_type": "input",
                "target_key": "case_to_account"
            },
            {
                "source_node": "load_data_tasks",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "aggregate_tasks",
                "target_type": "input",
                "target_key": "tasks"
            },
            {
                "source_node": "load_data_sessions",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "aggregate_sessions",
                "target_type": "input",
                "target_key": "sessions"
            },
            {
                "source_node": "load_and_aggregate_emails",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "concat_all",
                "target_type": "input",
                "target_key": "emails"
            },
            {
                "source_node": "aggregate_tasks",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "concat_all",
                "target_type": "input",
                "target_key": "tasks"
            },
            {
                "source_node": "aggregate_sessions",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "concat_all",
                "target_type": "input",
                "target_key": "sessions"
            },
            {
                "source_node": "concat_all",
                "source_key": "*.csv",
                "source_type": "output",
                "target_node": "upload_to_redshift",
                "target_type": "input",
                "target_key": "final"
            }
        ],
        "nodes": [
            {
                "name": "load_case_to_account",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "load_case_to_account",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.load_data_from_redshift(\"case_to_account.sql\")'",
                    "inputs": {},
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "load_data_tasks",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "load_data_tasks",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.load_data_from_s3('\\\"Data_Science\\/Text_Data\\/Salesforce\\/Task\\/\\\",days_back=\"1\"')'",
                    "inputs": {},
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "load_data_sessions",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "load_data_sessions",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.load_data_from_s3('\\\"Data_Science\\/Text_Data\\/Salesforce\\/TechnicalInfo\\/\\\",days_back=\"1\"')'",
                    "inputs": {},
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "load_and_aggregate_emails",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "load_and_aggregate_emails",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.aggregate('source=\\\"emails\\\",days_back=1')'",
                    "inputs": {
                        "case_to_account": []
                    },
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "aggregate_sessions",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "aggregate_sessions",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.aggregate('source=\\\"sessions\\\"')'",
                    "inputs": {
                        "sessions": []
                    },
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "aggregate_tasks",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "aggregate_tasks",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.aggregate('source=\\\"tasks\\\"')'",
                    "inputs": {
                        "tasks": []
                    },
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "concat_all",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "concat_all",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.concat_all()'",
                    "inputs": {
                        "emails": [],
                        "sessions": [],
                        "tasks": []
                    },
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            },
            {
                "name": "upload_to_redshift",
                "type": "execution",
                "template": {
                    "environment": "01742a18-656d-faec-4cb6-acb1d06aa5c5",
                    "commit": "prod",
                    "step": "upload_to_redshift",
                    "image": "yotamljfrog/ds-general-image:0.1",
                    "command": "pip install -r requirements.txt\npython -c 'import main; main.upload_to_redshift('\\\"simple_intent_alltime\\\",append=\"1\"')'",
                    "inputs": {
                        "final": []
                    },
                    "parameters": {},
                    "runtime_config": {},
                    "inherit_environment_variables": True,
                    "environment_variable_groups": [],
                    "tags": [
                        "prod"
                    ],
                    "time_limit": 0,
                    "environment_variables": {}
                },
                "on_error": "stop-all"
            }
        ],
        "project": "017b2460-05ed-e5b6-8849-b1090f517460",
        "tags": [
            "prod"
        ],
        "parameters": {},
        "title": "key_terms_pipeline"
    },
)
if resp.status_code == 400:
    raise RuntimeError(resp.json())
resp.raise_for_status()
data = resp.json()
