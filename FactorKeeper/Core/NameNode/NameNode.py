from Core.Conf.DatabaseConf import DBConfig
from Core.Conf.FactorConf import FactorConf
from Core.Conf.TickDataConf import TickDataConf
from Core.Conf.PathConf import Path
from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.DAO.TickDataDao import TickDataDao
from Core.Logger.Logger import Logger
from Core.Error.Error import Error
from Core.NameNode.NameNodeImpl.Initializer import Initializer, LogInitializer
from Core.NameNode.TaskManager.TaskManager import TaskManager
from Core.NameNode.WorkerManager.WorkerManager import WorkerManager
from Core.NameNode.TaskManager.FactorUpdateTask import UpdateFactorTaskHandler
from Core.NameNode.TaskManager.TickDataUpdateTask import TickDataUpdateTaskHandler
import threading


class Master(object):
    def __init__(self):
        LogInitializer.init_log_dir()
        self.logger = Logger(Path.NAMENODE_MANAGER_LOG_PATH, "NameNode")

        # start log
        self.logger.log_info("starting name node...")

        # create db engine
        self.db_engine = DBConfig.create_default_sa_engine_without_pool()

        # create name node variables
        self.factor_dao = FactorDao(self.db_engine, self.logger)
        self.tick_dao = TickDataDao(self.db_engine, self.logger)
        self.lock = threading.Lock()

        # init name node
        self.initializer = Initializer(self.db_engine, self.logger)
        self.initializer.init_master_node()

        # init managers
        self.logger.log_info("initializing managers...")
        self.worker_manager = WorkerManager(self.logger)
        self.task_manager = TaskManager(self.worker_manager, self.db_engine, self.logger)

        # install task handlers
        self.task_manager.install_task_handler(UpdateFactorTaskHandler)
        self.task_manager.install_task_handler(TickDataUpdateTaskHandler)
        self.logger.log_info("successfully initialized managers.")

    def register_worker(self, host, port, cores, worker_version):
        return self.worker_manager.register_worker(host, port, cores, worker_version)

    def update_worker(self, host, port, tasks, update_time):
        return self.worker_manager.update_worker(host, port, tasks, update_time)

    def list_workers(self):
        return self.worker_manager.list_workers()

    def list_tasks(self):
        return self.task_manager.list_tasks()

    def list_finished_tasks(self, task_num):
        return self.task_manager.get_finished_tasks(task_num)

    def list_stock_status(self, stock_code):
        err, updated_dates = self.tick_dao.list_updated_dates(stock_code)
        if err == Error.ERROR_TICK_DATA_NOT_EXISTS:
            updated_dates = []
        elif err:
            return err

        updated_strs = [str(date) for date in updated_dates]

        err, total_dates = self.tick_dao.list_available_tick_dates(stock_code)
        if err:
            return err, None

        to_update_dates = sorted(list(set(total_dates) - set(updated_dates)))
        to_update_strs = [str(date) for date in to_update_dates]

        status = "Stock Code: {} <br>".format(stock_code) +\
                 "Updated Dates: {} <br>".format(updated_strs) +\
                 "To Update Dates: {} <br>".format(to_update_strs)

        return Error.SUCCESS, status

    def call_back_update_factor_task(self, factor, version, stock_code, day, df, task_id):
        return self.task_manager.callback_task(UpdateFactorTaskHandler, factor=factor, version=version,
                                               stock_code=stock_code, date=day, data_frame=df, task_id=task_id)

    def call_back_update_tick_data_task(self, stock_code, day, df, task_id):
        return self.task_manager.callback_task(TickDataUpdateTaskHandler, stock_code=stock_code,
                                               date=day, data_frame=df, task_id=task_id)

    def create_factor(self, factor, code_file):
        """
        :param factor:
        :param code_file:
        :return: err_code
        """
        err = self.factor_dao.create_factor(factor)
        if err and err != Error.ERROR_FACTOR_ALREADY_EXISTS:
            return err

        err = self.factor_dao.create_factor_version(factor, FactorConf.FACTOR_INIT_VERSION, code_file)
        if err == Error.ERROR_FACTOR_VERSION_ALREADY_EXISTS:
            return Error.ERROR_FACTOR_ALREADY_EXISTS
        else:
            return err

    def create_group_factor(self, factors, code_file):
        """
        :param factors:
        :param code_file:
        :return: err_code
        """
        if len(factors) == 0:
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID

        err, group_factor_name = self.factor_dao.create_group_factor(factors)
        if err and err != Error.ERROR_FACTOR_ALREADY_EXISTS:
            return err
        elif err == Error.ERROR_FACTOR_ALREADY_EXISTS:
            group_factor_name = FactorConf.get_group_factor_name(factors)

        err = self.factor_dao.create_group_factor_version(group_factor_name, factors, FactorConf.FACTOR_INIT_VERSION,
                                                          code_file)
        if err == Error.ERROR_FACTOR_VERSION_ALREADY_EXISTS or err == Error.ERROR_FACTOR_NOT_EXISTS:
            return Error.ERROR_FACTOR_ALREADY_EXISTS
        else:
            return err

    def create_group_factor_version(self, factors, version, code_file):
        """
        :param factors:
        :param version:
        :param code_file:
        :return: err_code
        """
        if len(factors) == 0:
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID

        group_factor_name = None
        for factor in factors:
            err, gfn = self.factor_dao.get_group_factor(factor)
            if err:
                return err

            if gfn is not None:
                if group_factor_name is None:
                    group_factor_name = gfn
                elif group_factor_name != gfn:
                    return Error.ERROR_GROUP_FACTOR_SOURCE_CONFLICT

        if group_factor_name is None:
            return Error.ERROR_GROUP_FACTOR_NOT_EXISTS

        return self.factor_dao.create_group_factor_version(group_factor_name, factors, version, code_file)

    def list_factor(self):
        """
        :return: err_code, list of factors
        """
        return self.factor_dao.list_factors()

    def create_version(self, factor, version, code_file):
        """
        :param factor:
        :param version:
        :param code_file:
        :return: err_code
        """
        return self.factor_dao.create_factor_version(factor, version, code_file)

    def list_versions(self, factor):
        """
        :param factor:
        :return: err_code, list of versions
        """
        return self.factor_dao.list_versions(factor)

    def create_stock_view(self, stock_view_name, relations):
        """
        :param stock_view_name:
        :param relations:
        :return: err_code
        """
        import re

        if not TickDataConf.is_stock_view(stock_view_name):
            return Error.ERROR_INVALID_STOCK_VIEW_NAME, "stock view name must end with '.VIEW'"
        res = re.match(r"[a-zA-Z_\-.0-9]+", stock_view_name)
        if res is None or stock_view_name != res.group(0):
            return Error.ERROR_INVALID_STOCK_VIEW_NAME, "stock view name must be consist of alphabet characters " \
                                                        "and '-', '_', '.'"

        return self.tick_dao.create_stock_view(stock_view_name, relations)

    def create_stock_linkage(self, factor, stock_code, version=None, ):
        """
        :param stock_code:
        :param factor:
        :param version:
        :return: err_code
        """

        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err

        return self.factor_dao.create_linkage(factor, version, stock_code)

    def list_linked_stocks(self, factor, version=None):
        """
        :param factor:
        :param version:
        :return: err_code, list of stocks
        """
        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err

        return self.factor_dao.list_linked_stocks(factor, version)

    def get_linkage_status(self, factor, version, stock_code):
        """
        :param factor:
        :param version:
        :param stock_code:
        :return: err_code, linkage status
        """
        err, updated_dates = self.factor_dao.list_updated_dates(factor, version, stock_code)
        if err:
            return err, None

        err, can_update_dates = self.tick_dao.list_available_tick_dates(stock_code)
        if err:
            return err

        to_update_dates = set(can_update_dates) - set(updated_dates)
        updated_dates = [str(date) for date in updated_dates]
        to_update_dates = [str(date) for date in to_update_dates]

        return Error.SUCCESS, "<br>Updated Dates:<br>{0}<br><br>To Update Dates:<br>{1}".\
            format(updated_dates, to_update_dates)

    def update_factor_result(self, factor, stock_code, version=None):
        """
        :param stock_code:
        :param factor:
        :param version:
        :return: err_code, task status
        """

        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err, None

        # update factor result
        return self._create_task(UpdateFactorTaskHandler, factor=factor, version=version,
                                 stock_code=stock_code)

    def load_factor_results(self, factor, stock_code, fetch_date, version=None):
        """
        :param stock_code:
        :param factor:
        :param version:
        :param fetch_date:
        :return: err_code, factor dataframe
        """
        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err, None

        return self.factor_dao.load_factor_result(factor, version, stock_code, fetch_date)

    def load_multi_factor_results(self, factors, stock_code, fetch_date):
        """
        :param factors:
        :param stock_code:
        :param fetch_date:
        :return: err_code, dataframe
        """

        if not isinstance(factors, dict):
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factors must be a list or tuple"

        for factor in factors:
            if not isinstance(factor, str):
                return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factor name must be string"

            version = factors[factor]
            if not (isinstance(version, str) or version is None):
                return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factor version must be string or None"

        for factor in list(factors.keys()):
            version = factors[factor]

            if version is None:
                err, factors[factor] = self.factor_dao.get_latest_version(factor)
                if err == Error.ERROR_FACTOR_NOT_EXISTS:
                    return err, "factor not exists({})".format(factor)
                elif err:
                    return err, None

        result_df = None
        for factor in factors:
            err, factor_df = self.factor_dao.load_factor_result(factor, factors[factor], stock_code, fetch_date,
                                                                with_time=result_df is None)
            if err == Error.ERROR_FACTOR_RESULT_NOT_EXISTS:
                return err, "factor result not exists({0}:{1})".format(factor, factors[factor])
            else:
                if result_df is None:
                    result_df = factor_df
                else:
                    result_df[factor] = factor_df[factor]

        return Error.SUCCESS, result_df

    def load_multi_factor_result_by_range(self, factors, stock_code, start_date, end_date):
        """
        :param factors:
        :param stock_code:
        :param start_date:
        :param end_date:
        :return: err_code, dataframe
        """

        if not isinstance(factors, dict):
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factors must be a list or tuple"

        for factor in factors:
            if not isinstance(factor, str):
                return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factor name must be string"

            version = factors[factor]
            if not (isinstance(version, str) or version is None):
                return Error.ERROR_PARAMETER_MISSING_OR_INVALID, "factor version must be string or None"

        for factor in list(factors.keys()):
            version = factors[factor]

            if version is None:
                err, factors[factor] = self.factor_dao.get_latest_version(factor)
                if err == Error.ERROR_FACTOR_NOT_EXISTS:
                    return err, "factor not exists({})".format(factor)
                elif err:
                    return err, None

        result_df = None
        df_dict = {}
        dates = None
        for factor in factors:
            err, factor_df = self.factor_dao.load_factor_result_by_range(factor, factors[factor], stock_code,
                                                                         start_date, end_date,
                                                                         with_time=True)
            if err == Error.ERROR_FACTOR_RESULT_NOT_EXISTS:
                return err, "factor result not exists({0}:{1})".format(factor, factors[factor])
            else:
                df_dict[factor] = factor_df
                dates = dates.union(set(factor_df['date'].tolist())) if dates is not None else \
                    set(factor_df['date'].tolist())

        if len(dates) == 0:
            return Error.ERROR_FACTOR_RESULT_NOT_EXISTS, None

        for factor in df_dict:
            df = df_dict[factor]
            df = df[df.date.isin(dates)]
            if result_df is None:
                result_df = df
            else:
                result_df[factor] = df[factor].tolist()

        return Error.SUCCESS, result_df

    def get_factor_update_status(self, factor, stock_code, version=None):
        """
        :param factor:
        :param stock_code:
        :param version:
        :return: err_code, update_status
        """
        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err, None

        return self._get_task_status(UpdateFactorTaskHandler, factor=factor, version=version,
                                     stock_code=stock_code)

    def stop_factor_update_progress(self, factor, stock_code, version=None):
        """
        :param factor:
        :param stock_code:
        :param version:
        :return: err_code
        """
        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err

        return self._stop_task(UpdateFactorTaskHandler, factor=factor, version=version,
                               stock_code=stock_code)

    def list_updated_dates(self, factor, stock_code, version=None):
        """
        :param factor:
        :param stock_code:
        :param version:
        :return: err_code, list of dates
        """
        if version is None:
            err, version = self.factor_dao.get_latest_version(factor)
            if err:
                return err

        return self.factor_dao.list_updated_dates(factor, version, stock_code)

    def _create_task(self, task_type, *args, **kwargs):
        return self.task_manager.new_task(task_type, *args, **kwargs)

    def _get_task_status(self, task_type, *args, **kwargs):
        return self.task_manager.query_task(task_type, *args, **kwargs)

    def _stop_task(self, task_type, *args, **kwargs):
        return self.task_manager.stop_task(task_type, *args, **kwargs)

    def stop_all_tasks(self):
        """
        :return: err_code
        """
        return self.task_manager.stop_all()

    def finish_task(self, task_id, finished=None, aborted=None, total=None):
        """
        :param task_id:
        :param finished:
        :param aborted:
        :param total:
        :return: err_code
        """
        return self.task_manager.finish_task(task_id, finished_task_num=finished, aborted_task_num=aborted,
                                             total_task_num=total)
