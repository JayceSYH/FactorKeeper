"""
    This file defines functions used to create/query master node meta info.
    You should not modify this file.
"""


from Core.Error.Error import Error
from Core.Conf.DatabaseConf import Schemas, Tables
from Core.NameNode.TaskManager.CommonTask import FinishedTask
import traceback
import pandas as pd


class MasterDao(object):
    def __init__(self, db_engine, logger):
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.db_engine = db_engine

    def add_finish_task(self, task_id, task_type, commit_time, finish_time, final_status, is_sub_task,
                        total_tasks=None, finished_num=None, aborted_num=None, worker_id=None):
        """
        Insert an finished task record to database
        :param task_id: task id
        :param task_type: task type
        :param commit_time: task commit time
        :param finish_time: task finish time
        :param final_status: task final status(finished/aborted)
        :param is_sub_task: is task a sub task
        :param total_tasks: number of total unit tasks
        :param finished_num: number ot finished unit tasks
        :param aborted_num: number of aborted unit tasks
        :param worker_id: last responsible worker's id
        :return: err_code
        """

        conn = self.db_engine.connect()
        try:
            insert_sql = """
                        INSERT INTO "{0}"."{1}"(task_id, task_type, commit_time, finish_time, final_status, 
                        total_sub_tasks, finished, aborted, is_sub_task, worker_id)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASKS)

            conn.execute(insert_sql, (task_id, task_type, commit_time, finish_time, final_status,
                                     total_tasks, finished_num, aborted_num, 1 if is_sub_task else 0, worker_id))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def _add_one_task_dependency(self, base_task_id, dep_task_id):
        """
        Insert a dependent task record to database
        :param base_task_id: main task id
        :param dep_task_id: dependency task id
        :return: err_code
        """

        conn = self.db_engine.connect()
        try:
            insert_sql = """
                                INSERT INTO "{0}"."{1}"(base_task_id, dependency_task_id)
                                VALUES(%s, %s)
                            """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASK_DEPENDENCY)

            conn.execute(insert_sql, (base_task_id, dep_task_id))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def add_task_dependency(self, base_task_id, dependency_list):
        """
        Insert a dependent task record to database
        :param base_task_id: main task id
        :param dependency_list: list of dependency task id
        :return: err_code
        """
        for dep_task_id in dependency_list:
            self._add_one_task_dependency(base_task_id, dep_task_id)

        return Error.SUCCESS

    def __df_to_task_obj(self, task_df):
        """
        Convert a dataframe to task object
        :param task_df:
        :return: err_code, task object
        """
        try:
            task_list = []
            for _, row in task_df.iterrows():
                task = FinishedTask(row['task_id'], row['task_type'], row['commit_time'], row['finish_time'],
                                    row['final_status'], total_tasks=row.get("total_sub_tasks"),
                                    finished_num=row.get("finished"), aborted_num=row.get("aborted"),
                                    worker_id=row.get("worker_id"))
                task_list.append(task)

            return Error.SUCCESS, task_list
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None

    def __add_dependencies(self, task, sa_conn):
        """
        add task dependencies to a task object
        :param task: task object
        :param sa_conn:
        :return: err_code
        """
        try:
            # get dependencies
            deps_task_id_df = pd.read_sql("""
                SELECT dependency_task_id FROM "{0}"."{1}"
                WHERE base_task_id='{2}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASK_DEPENDENCY, task.finish_task_id),
                                          con=sa_conn)

            if deps_task_id_df.shape[0] == 0:
                return Error.SUCCESS

            task_id_list = deps_task_id_df['dependency_task_id'].tolist()
            for task_id in task_id_list:
                err, dep_task = self.__get_task_with_dependencies(task_id, sa_conn)
                if not err:
                    task.add_dependency(dep_task)

            return Error.SUCCESS
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED

    def __get_task_with_dependencies(self, task_id, sa_conn):
        """
        Get a task object with its dependency tasks
        :param task_id:
        :param sa_conn:
        :return: err, task object
        """
        try:
            task_df = pd.read_sql("""
                SELECT * FROM "{0}"."{1}" WHERE task_id='{2}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASKS, task_id), con=sa_conn)
            if task_df.shape[0] == 0:
                return Error.ERROR_TASK_NOT_EXISTS, None

            err, tasks = self.__df_to_task_obj(task_df)
            if err:
                return err, None

            task = tasks[0]
            err = self.__add_dependencies(task, sa_conn)
            if err:
                return err, None

            return Error.SUCCESS, task
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED

    def get_recent_finished_tasks(self, task_num):
        """
        Get recent finished tasks with limit "task_num"
        :param task_num: number of recent finished tasks to be fetched
        :return: err_code, task object list
        """
        conn = self.db_engine.connect()
        try:
            # get base tasks
            task_df = pd.read_sql("""
                SELECT * FROM "{0}"."{1}" WHERE is_sub_task=0 ORDER BY id DESC LIMIT {2}
            """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASKS, task_num), con=conn)

            err, task_list = self.__df_to_task_obj(task_df)
            if err:
                return err, None

            # add dependencies
            for task in task_list:
                self.__add_dependencies(task, conn)

            return Error.SUCCESS, task_list

        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()
