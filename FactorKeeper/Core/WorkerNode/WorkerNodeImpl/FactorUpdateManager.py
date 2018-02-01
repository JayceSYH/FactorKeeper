from Core.DAO.TickDataDao import TickDataDao
from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.Conf.TickDataConf import TickDataConf
from Core.WorkerNode.WorkerNodeImpl.WorkerTaskManager import TaskGroup, TaskConst, Task
from Core.Error.Error import Error
from Core.Conf.PathConf import Path
from Core.WorkerNode.WorkerNodeImpl.Message import FinishACKMessage, KillMessage, MessageLogger
from Core.WorkerNode.WorkerNodeImpl.FileSaver import FileSaver
from Core.WorkerNode.WorkerNodeImpl.MessageSender import MessageSender
import sys, os, traceback, datetime, threading
import pandas as pd


class FactorUpdateManager(object):
    """
        FactorUpdateManager calculate to update dates and split a factor update task to unit tasks
        which only update one day of factor data
    """

    def __init__(self, task_manager, db_engine, logger):
        self._logger = logger
        self._task_manager = task_manager
        self._db_engine = db_engine
        self._tick_dao = TickDataDao(db_engine, logger)
        self._factor_dao = FactorDao(db_engine, logger)
        threading.Thread(target=lambda: self._task_manager.run()).start()

    def update_linkage(self, factor, version, stock_code, task_id):
        """
        update factor data
        :return: err_code
        """

        generator_path = "{0}/{1}/{2}".format(Path.FACTOR_GENERATOR_BASE, factor, version)

        # download file if not exists
        if not os.path.exists(generator_path):
            # download script
            err, code_file = self._factor_dao.get_factor_version_code(factor, version)
            if err:
                return err

            err = FileSaver.save_code_to_fs(factor, version, bytes(code_file), self._logger)
            if err:
                return err, None

        # fetch updated dates
        err, updated_days = self._factor_dao.list_updated_dates(factor, version, stock_code)
        if err:
            return err, None

        # fetch tick data dates
        err, tick_days = self._tick_dao.list_updated_dates(stock_code)

        # get to update dates
        to_update_days = sorted(list(set(tick_days) - set(updated_days)))

        # convert tick data to factors
        var_list = [(factor, version, stock_code, day)
                    for day in to_update_days]

        update_item_num = len(var_list)
        if update_item_num == 0:
            return Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE, 0

        task_group = TaskGroup(TaskConst.TaskType.UPDATE_FACTOR_TASK, task_id)
        for var in var_list:
            task = Task(TaskConst.TaskType.UPDATE_FACTOR_TASK, self.__make_task_sub_id(*var))
            task.set_target(update_day_factor_in_async, args=var)
            task_group.add_task(task)

        self._task_manager.apply_task_group(task_group)

        return Error.SUCCESS, update_item_num

    @staticmethod
    def __make_task_sub_id(*args):
        return "_".join([str(arg) for arg in args])

    def query_update_status(self, task_id):
        return self._task_manager.query_group_progress(task_id)

    def stop_task_group(self, group_id):
        return self._task_manager.stop_task_group(group_id)

    def stop_all_factor_update_task_groups(self):
        return self._task_manager.stop_task_groups(task_type=TaskConst.TaskType.UPDATE_FACTOR_TASK)


def update_day_factor_in_async(factor, version, stock_code, day, *args, **kwargs):
    """
    update factor of a single day
    :param factor:
    :param version:
    :param stock_code:
    :param day:
    :param args:
    :param kwargs:
    :return:
    """
    
    from Core.Conf.DatabaseConf import DBConfig

    # get task info
    task_id = kwargs.get(TaskConst.TaskParam.TASK_ID)
    task_queue = kwargs.get(TaskConst.TaskParam.TASK_MANAGER_QUEUE)
    task_group_id = kwargs.get(TaskConst.TaskParam.TASK_GROUP_ID)
    log_stack = kwargs.get(TaskConst.TaskParam.LOG_STACK)
    
    # get global variable
    db_engine = DBConfig.default_config().create_default_sa_engine_without_pool()

    # add sys path if not exists
    if Path.FACTOR_GENERATOR_BASE not in sys.path:
        sys.path.append(Path.FACTOR_GENERATOR_BASE)

    # set task status
    _task_aborted = False

    # set logger
    logger = MessageLogger(task_id, task_group_id, log_stack, task_queue)

    # log start info
    logger.log_info("factor update task starting...")

    try:
        # create database connection
        tick_dao = TickDataDao(db_engine, logger)
        factor_dao = FactorDao(db_engine, logger)

        # fetch data of a day
        daytime = datetime.datetime(year=day.year, month=day.month, day=day.day)
        err, day_df = tick_dao.load_updated_tick_data(stock_code, daytime)

        if err:
            logger.log_error("({}) failed to fetch tick data".format(err))
            _task_aborted = True
            return err

        if day_df.shape[0] < 1000:
            logger.log_info("too few tick data({} ticks)".format(day_df.shape[0]))
            return Error.SUCCESS

        err, link_id = factor_dao.get_linkage_id(factor, version, stock_code)
        if err:
            _task_aborted = True
            return err

        # add factor generator script path to python sys path
        generator_path = "{0}/{1}/{2}".format(Path.FACTOR_GENERATOR_BASE, factor, version)

        # run python script
        generator_module_name = [f for f in os.listdir(generator_path) if f != "__init__.py"][0]
        generator_module_path = "{0}/{1}".format(generator_path, generator_module_name)
        if not os.path.isdir(generator_module_path):
            if generator_module_name.endswith(".py"):
                generator_module_name = generator_module_name[:-3]
            else:
                logger.log_error("Unrecognized file type: {}".format(generator_module_name))
                _task_aborted = True
                return Error.ERROR_UNRECOGNIZED_FILE_TYPE

        try:
            import importlib
            generator_module = importlib.import_module("{0}.{1}.{2}".format(factor, version,
                                                                            generator_module_name))
        except:
            logger.log_error(traceback.format_exc())
            _task_aborted = True
            return Error.ERROR_FAILED_TO_LOAD_FACTOR_GENERATOR_MODULE

        factor_generator = generator_module.factor_generator

        # execute factor generator
        try:
            factor_value = factor_generator(day_df, stock_code, day)

            if isinstance(factor_value, pd.DataFrame):
                signature = [factor]
                if factor_dao.is_group_factor(factor):
                    err, signature = factor_dao.get_sub_factors(factor, version)
                    if err:
                        _task_aborted = True
                        return err

                if not set(factor_value.columns).issuperset(set(signature)):
                    _task_aborted = True
                    return Error.ERROR_GROUP_FACTOR_SIGNATURE_NOT_MATCHED

                factor_value = factor_value[signature]
                factor_value['date'] = day_df['date'].tolist()
                factor_value['datetime'] = day_df['datetime'].tolist()

            elif not isinstance(factor_value, list) or len(factor_value) != TickDataConf.TICK_LENGTH:
                _task_aborted = True
                logger.log_error("invalid factor result format:\n" + str(factor_value))
                return Error.ERROR_INVALID_FACTOR_RESULT

            else:
                factor_value = pd.DataFrame({"datetime": day_df['datetime'], factor: factor_value, "date": day_df['date']})

            err, msg = MessageSender.send_factor_result_to_master(factor, version, stock_code, day, factor_value,
                                                                  task_group_id, logger)
            if err:
                logger.log_error("Error occurred during tick update callback: {0} {1}".format(err, msg))
                if err == Error.ERROR_TASK_NOT_EXISTS:
                    task_queue.put(KillMessage(task_id))
                    return

                _task_aborted = True
                return err
        except:
            logger.log_error(traceback.format_exc())
            _task_aborted = True
            return Error.ERROR_FACTOR_GENERATE_FAILED

    except:
        _task_aborted = True
        logger.log_error(traceback.format_exc())
        return Error.ERROR_FACTOR_GENERATE_FAILED

    finally:
        logger.log_info("task finished")
        task_queue.put(FinishACKMessage(task_id, aborted=_task_aborted))
