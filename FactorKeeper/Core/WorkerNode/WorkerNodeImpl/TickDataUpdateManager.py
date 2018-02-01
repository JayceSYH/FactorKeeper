from Core.DAO.TickDataDao import TickDataDao
from Core.Error.Error import Error
from Core.WorkerNode.WorkerNodeImpl.WorkerTaskManager import Task, TaskGroup, TaskConst
from Core.WorkerNode.WorkerNodeImpl.Message import FinishACKMessage, KillMessage, MessageLogger
from Core.Conf.TickDataConf import TickDataConf
from Core.WorkerNode.WorkerNodeImpl.MessageSender import MessageSender
import traceback
import pandas as pd


class TickDataUpdateManager(object):
    """
       TickDataUpdateManager calculate to update dates and split a tick data update task to unit tasks
       which only update one day of factor data
   """

    def __init__(self, task_manager, db_engine, logger):
        self.logger = logger
        self.db_engine = db_engine
        self.task_manager = task_manager
        self.tick_dao = TickDataDao(db_engine, logger)

    def update_stock_data(self, stock_code, task_id):
        err, available_dates = self.tick_dao.list_available_tick_dates(stock_code)
        if err:
            return err, None

        if len(available_dates) == 0:
            return Error.ERROR_TICK_DATA_NOT_AVAILABLE, None

        err, new_db_tick_dates = self.tick_dao.list_updated_dates(stock_code)
        if err:
            return err, None

        dates_to_update = sorted(list(set(available_dates) - set(new_db_tick_dates)))
        dates_to_update = list(dates_to_update)
        update_item_num = len(dates_to_update)
        if update_item_num == 0:
            return Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE, 0

        task_group = TaskGroup(TaskConst.TaskType.UPDATE_TICK_DATA_TASK, task_id)
        for day in dates_to_update:
            task = Task(TaskConst.TaskType.UPDATE_TICK_DATA_TASK, "{0}:{1}".format(stock_code, day))
            task.set_target(update_stock_data_async, args=(stock_code, day))
            task_group.add_task(task)

        self.task_manager.apply_task_group(task_group)

        return Error.SUCCESS, update_item_num

    def query_update_status(self, task_id):
        return self.task_manager.query_group_progress(task_id)

    def get_factor_update_task_list(self):
        return self.task_manager.get_factor_update_group_list()

    def stop_task_group(self, group_id):
        return self.task_manager.stop_task_group(group_id)

    def stop_all_tick_update_task_groups(self):
        return self.task_manager.stop_task_groups(task_type=TaskConst.TaskType.UPDATE_TICK_DATA_TASK)


def update_stock_data_progress(stock_code, day, logger, db_engine):
    tick_dao = TickDataDao(db_engine, logger)

    try:
        err, stock_df = tick_dao.load_data_from_outer_source(stock_code, day)

        if err:
            logger.log_error("failed to load stock data from old database on {}".format(day))
            return err, None
        stock_df = stock_df.drop(['index', 'windcode'], axis=1)
    except:
        logger.log_error(traceback.format_exc())
        return Error.ERROR_SERVER_INTERNAL_ERROR

    stock_df.set_index('datetime', inplace=True)
    stock_df = stock_df[~stock_df.index.duplicated()]
    stock_df = stock_df.resample('s').sum()
    stock_df.fillna(method="ffill", inplace=True)

    day_str = str(day)
    stock_df = pd.DataFrame(stock_df,
                            index=pd.date_range(day_str + ' 09:30:03', day_str + ' 11:30:00', freq='3s').append(
                                pd.date_range(day_str + ' 13:00:00', day_str + ' 14:56:57', freq='3s'))). \
        fillna(method='ffill')
    stock_df.fillna(method='bfill', inplace=True)
    stock_df['date'] = day
    stock_df['datetime'] = stock_df.index

    if stock_df.shape[0] != TickDataConf.TICK_LENGTH:
        logger.log_error("tick data result format incorrect, {} rows found".format(stock_df.shape[0]))
        return Error.ERROR_TICK_RESULT_INCORRECT, None

    return Error.SUCCESS, stock_df


def update_stock_view_progress(stock_code, day, logger, db_engine):
    tick_dao = TickDataDao(db_engine, logger)

    try:
        err, relation = tick_dao.get_stock_view_relation(stock_code)
        if err:
            return err, None

        ret_df = None

        stocks = relation.keys()
        for dep_stock in stocks:
            columns = relation[dep_stock]
            err, dep_stock_df = tick_dao.load_updated_tick_data(dep_stock, day, columns=(columns + ["datetime"]))
            if err:
                return err, None

            if dep_stock_df.shape[0] != TickDataConf.TICK_LENGTH:
                logger.log_error("tick data result format incorrect, {} rows found".format(dep_stock_df.shape[0]))
                return Error.ERROR_TICK_RESULT_INCORRECT, None

            if ret_df is None:
                ret_df = dep_stock_df[['datetime']]
            ret_columns = ["{0}_{1}".format(col, dep_stock) for col in columns]
            ret_df[ret_columns] = dep_stock_df[columns]

        ret_df['date'] = day
        print("set date:{}".format(day))
    except:
        logger.log_error(traceback.format_exc())
        return Error.ERROR_SERVER_INTERNAL_ERROR, None

    if ret_df is None:
        return Error.ERROR_SERVER_INTERNAL_ERROR, None

    return Error.SUCCESS, ret_df


def update_stock_data_async(stock_code, day, *args, **kwargs):
    """
    更新tick数据
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

    # set task status
    _task_aborted = False

    # set logger
    logger = MessageLogger(task_id, task_group_id, log_stack, task_queue)

    # log start info
    logger.log_info("factor update task starting...")

    try:
        if TickDataConf.is_stock_view(stock_code):
            err, stock_df = update_stock_view_progress(stock_code, day, logger, db_engine)
        else:
            err, stock_df = update_stock_data_progress(stock_code, day, logger, db_engine)

        if err:
            _task_aborted = True
            return err

        err, msg = MessageSender.send_tick_data_result_to_master(stock_code, day, stock_df, task_group_id, logger)
        if err == Error.ERROR_TASK_NOT_EXISTS:
            task_queue.put(KillMessage(task_id))
            _task_aborted = True
            return Error.ERROR_TASK_NOT_EXISTS
        elif err:
            logger.log_error("Error occurred during tick update callback: {0} {1}".format(err, msg))
            _task_aborted = True
            return Error.ERROR_FAILED_TO_CALLBACK_MASTER

    except:
        logger.log_error(traceback.format_exc())
        _task_aborted = True
    finally:
        task_queue.put(FinishACKMessage(task_id, aborted=_task_aborted))
