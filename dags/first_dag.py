# import Class DAG
from airflow import DAG
# import modul datetime untuk mendefinisikan waktu eksekusi
from datetime import datetime, timedelta
# import operator yang akan digunakan
from airflow.operators.bash import BashOperator

# Default configuration/arguments for dags
default_args = {
  'owner': 'faizal',
  'retries': 5,
  'retry_delay': timedelta(minutes=2)
}

# Define a DAG
with DAG(
  dag_id="first_dag_v5",
  default_args=default_args,
  description="This is our first dag",
  start_date = datetime(2022,10,19,2),
  schedule_interval="@daily"
) as dag:
  task1 = BashOperator(
    task_id = "first_task",
    bash_command="echo hello world, this is the first task"
  )

  task2 = BashOperator(
      task_id = "second_task",
      bash_command = "echo ini task 2, dijalankan setelah task 1 tampan"
  )

  task3 = BashOperator(
      task_id = "third_task",
      bash_command = "echo ini task 3, dijalankan juga setelah task 1 tampan"
  )

  # set task method 1
  # task1.set_downstream(task2)
  # task1.set_downstream(task3)
 
  # set task method 2
  # task1 >> task2
  # task1 >> task3 

  # set task method 3
  task1 >> [task2, task3]