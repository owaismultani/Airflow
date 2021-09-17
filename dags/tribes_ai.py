"""
This file creates Dag in apache airflow. **This file expects a DagFactory.py file in airflow dags folder**
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../projects/tribes.ai"))
from project_config import DAG_NAME, HISTORICAL_DATA_SINCE, SCHEDULE_CRON, USERS, LOGGER
from src.user_data_generator import UserDataGenerator
from src.save_to_neo4j import SaveToNeo4J


default_args = {
    'owner': 'tribes.ai',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=HISTORICAL_DATA_SINCE),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}
LOGGER.info(f"Creating DAG {DAG_NAME} with interval {SCHEDULE_CRON}")
with DAG(DAG_NAME, default_args=default_args, schedule_interval=SCHEDULE_CRON,
         is_paused_upon_creation=False, catchup=True, ) as dag:
    get_user_data = PythonOperator(task_id='get_user_data', python_callable=UserDataGenerator().get_user_data,
                                   op_kwargs={"users": USERS, "save": True}, dag=dag
                                   )

    save_to_neo4j = PythonOperator(task_id='save_to_neo4j', python_callable=SaveToNeo4J().send_data_to_neo4j, dag=dag,
                                   depends_on_past=True,)

    get_user_data >> save_to_neo4j

LOGGER.info(f"Successfully created DAG {DAG_NAME}")



