from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
"owner" : "faizal",
"retries" : 5,
"retry_delay" : timedelta(minutes=5)
}


def greet_name_age(ti):
  first_name = ti.xcom_pull("get_name", key="first_name")
  last_name = ti.xcom_pull("get_name", key="last_name")
  age = ti.xcom_pull("get_age", key="age")

  print(f"hi juga broo, nama saya {first_name} {last_name} umur saya {age}")

def get_name(ti):
  ti.xcom_push(key='first_name', value="Faizal")
  ti.xcom_push(key='last_name', value="Addi")

def get_age(ti):
  ti.xcom_push(key='age', value=21)

with DAG(
  default_args=default_args,
  dag_id="dag_with_python_operator_v7",
  description="Dag with python operator",
  start_date=datetime(2022,10,19),
  schedule_interval='@daily'

) as dag:
  task1 = PythonOperator(
    task_id="greet",
    python_callable=greet_name_age

  )

  task2 = PythonOperator(
    task_id="get_name",
    python_callable=get_name
  )

  task3 = PythonOperator(
    task_id="get_age",
    python_callable=get_age
  )

  [task2, task3]>> task1
