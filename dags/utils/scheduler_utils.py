from typing import Callable
from airflow import DAG
from airflow.operators.python import PythonOperator
from numpy import sort


class DagManager:

    def __init__(self, args: dict):
        self.__default_args = args
        self.__dag = None
        self.__tasks = {}

    def create_dag(
        self, dag_id: str, description: str, schedule_interval: str
    ) -> None:
        if self.__dag is not None:
            raise ValueError(
                "Dag is already created. Use new DagManager for creating new dag."
            )

        self.__dag = DAG(
            dag_id=dag_id,
            default_args=self.__default_args,
            description=description,
            schedule_interval=schedule_interval,
        )

    def __add_task(self, task: PythonOperator, task_place: int):

        if task_place in self.__tasks:
            raise ValueError("Already exists task with this place")

        task[task_place] = task

    def create_task_for_dag(
        self,
        taks_id: str,
        func: Callable,
        func_args: dict,
        task_place: int,
    ) -> None:
        task = PythonOperator(
            task_id=taks_id,
            python_callable=func,
            priority_weight=task_place
            op_args=func_args,
            dag=self.__dag,
        )
        self.__add_task(task, task_place)

    def add_dag_to_airflow(self) -> None:

        self.__tasks = dict(
            sorted(self.__tasks.items(), key=lambda t: t[0])
        )
