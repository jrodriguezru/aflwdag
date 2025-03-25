from datetime import datetime, timedelta

from airflow.models.dag import DAG

from airflow.operators.bash import BashOperator

from airflow.operators.python import PythonOperator


def print_hello():
    print('hello world')


with DAG(
    'My_dag',
    default_args={
        "owner": 'juanrr8765',
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="My first DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 3, 15),
    tags=['my_dag']
) as dag:

    t1 = BashOperator(
        task_id='Date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command="sleep 3",
    )

    t3 = PythonOperator(
        task_id='hellow',
        python_callable=print_hello
    )

    t1 >> [t2, t3]
