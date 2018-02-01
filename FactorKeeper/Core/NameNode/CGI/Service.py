"""
    This file defines services of file keeper, you may modify it if needed.
"""


from Core.Conf.ProtoConf import ProtoConf
from Util.ServiceUtil.Response import ResponseMaker
from Core.Error.Error import Error
from Util.ServiceUtil.Debug import ServiceDebugger
import datetime
import traceback
import pandas as pd


def define_name_node_service(app, name_node):
    from flask import request

    resp_maker = ResponseMaker()
    ServiceDebugger.set_debug(True)

    @app.route("/worker", methods=['POST'])
    @ServiceDebugger.debug()
    def register_worker():
        """
        register a new worker
        :return: return message
        """
        header = request.form.get("HEADER")
        if header != ProtoConf.WORKER_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        try:
            host = request.form.get("host")
            port = int(request.form.get("port"))
            cores = int(request.form.get("cores"))
            version = request.form.get("version")
        except:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)
        err, min_version = name_node.register_worker(host, port, cores, version)
        if err:
            return resp_maker.make_response(err, min_version)
        else:
            return resp_maker.make_response(err)

    @app.route("/worker", methods=['PUT'])
    @ServiceDebugger.debug(disable=True)
    def update_worker():
        """
        update worker info
        :return return message
        """
        header = request.form.get("HEADER")
        if header != ProtoConf.WORKER_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        host = request.form.get("host")
        port = request.form.get("port")
        tasks = request.form.get("tasks").split(ProtoConf.TASK_SPLITTER)
        tasks = [task.strip() for task in tasks if task.strip()]
        update_time = datetime.datetime.strptime(request.form.get("update_time"), ProtoConf.DATETIME_FORMAT)

        err = name_node.update_worker(host, port, tasks, update_time)
        return resp_maker.make_response(err)

    @app.route("/worker", methods=['GET'])
    @ServiceDebugger.debug()
    def list_workers():
        """
        list all active workers
        :return: return message
        """
        err, workers = name_node.list_workers()
        print(workers)
        if len(workers) > 0:
            worker_str = "<br>".join([str(worker) for worker in workers])
        else:
            worker_str = "No worker registered"

        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, worker_str)

    @app.route("/task", methods=['GET'])
    @ServiceDebugger.debug()
    def list_tasks():
        """
        list all running/waiting tasks
        :return: return message
        """
        err, tasks = name_node.list_tasks()
        if len(tasks) > 0:
            task_str = ("<br>" + "*" * 100).join([str(task) for task in tasks])
        else:
            task_str = "No task committed"

        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, task_str)

    @app.route("/finished_task", methods=['GET'])
    @ServiceDebugger.debug()
    def list_finished_tasks():
        """
        list recent finished tasks, you may reset the number of tasks to be shown
        :return: return message
        """
        err, tasks = name_node.list_finished_tasks(5)  # you may change the number of tasks to be shown
        if err:
            return resp_maker.make_response(err)

        if len(tasks) > 0:
            task_str = ("<br>" + "*" * 100).join([str(task) for task in tasks])
        else:
            task_str = "No task finished"

        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, task_str)

    @app.route("/stock/<stock_code>", methods=['GET'])
    @ServiceDebugger.debug()
    def list_stock_status(stock_code):
        """
        list stock status
        :return: return message
        """
        err, stock_status = name_node.list_stock_status(stock_code)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, stock_status)

    @app.route("/worker/call_back/update_factor/update", methods=['POST'])
    @ServiceDebugger.debug()
    def factor_update_call_back():
        """
        update factor data callback called by workers
        :return: return message
        """
        header = request.form.get("HEADER")
        if header != ProtoConf.CALLBACK_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        factor = request.form.get("factor")
        version = request.form.get("version")
        stock_code = request.form.get("stock_code")
        day = request.form.get("date")
        day = datetime.datetime.strptime(day, "%Y-%m-%d")
        try:
            df_json = request.form.get("data_frame")
            df = pd.read_json(df_json)
        except:
            return resp_maker.make_response(Error.ERROR_SERVER_INTERNAL_ERROR)
        task_id = request.form.get("task_id")

        err = name_node.call_back_update_factor_task(factor, version, stock_code, day, df, task_id)
        return resp_maker.make_response(err)

    @app.route("/worker/call_back/update_tick_data/update", methods=['POST'])
    @ServiceDebugger.debug()
    def update_tick_data_call_back():
        """
        update tick data call back called by workers
        :return: return message
        """
        header = request.form.get("HEADER")
        if header != ProtoConf.CALLBACK_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        stock_code = request.form.get("stock_code")
        day = request.form.get("date")
        day = datetime.datetime.strptime(day, "%Y-%m-%d")
        try:
            df_json = request.form.get("data_frame")
            df = pd.read_json(df_json)
        except:
            return resp_maker.make_response(Error.ERROR_SERVER_INTERNAL_ERROR)
        task_id = request.form.get("task_id")

        err = name_node.call_back_update_tick_data_task(stock_code, day, df, task_id)
        return resp_maker.make_response(err)

    @app.route("/worker/call_back/finish", methods=['POST'])
    @ServiceDebugger.debug()
    def finish_task():
        """
        finish a running task
        :return: return message
        """
        header = request.form.get("HEADER")
        if header != ProtoConf.CALLBACK_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        finished_num = request.form.get("finished")
        aborted_nm = request.form.get("aborted")
        total = request.form.get("total")
        task_id = request.form.get("task_id")
        if not task_id:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err = name_node.finish_task(task_id, finished=finished_num, aborted=aborted_nm, total=total)
        return resp_maker.make_response(err)

    @app.route("/stock_view", methods=['POST'])
    @ServiceDebugger.debug()
    def create_stock_view():
        """
        create a stock view
        :return: return message
        """
        import json

        stock_view_name = request.form.get("stock_view_name")
        stock_view_relation_json = request.form.get("stock_view_relation")

        try:
            relation = json.loads(stock_view_relation_json)
        except:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err, msg = name_node.create_stock_view(stock_view_name, relation)
        return resp_maker.make_response(err, msg)

    @app.route("/factor", methods=["POST"])
    @ServiceDebugger.debug()
    def create_factor():
        """
        create a factor
        :return: return message
        """
        # TODO: validate input
        factor = request.form.get("factor")
        if "code" not in request.files:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)
        code_file = request.files['code'].read()

        err = name_node.create_factor(factor, code_file)
        return resp_maker.make_response(err)

    @app.route("/group_factor", methods=["POST"])
    @ServiceDebugger.debug()
    def create_group_factor():
        """
        create a group factor
        :return: return message
        """
        import json

        try:
            factors = request.form.get("factors")
            factors = json.loads(factors)
            if "code" not in request.files:
                return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)
            code_file = request.files['code'].read()
        except:
            traceback.print_exc()
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err = name_node.create_group_factor(factors, code_file)
        return resp_maker.make_response(err)

    @app.route("/factor", methods=['GET'])
    @ServiceDebugger.debug()
    def list_factors():
        """
        list all factors
        :return:
        """
        err, factors = name_node.list_factor()
        if not err:
            return resp_maker.make_response(err, str(factors))
        else:
            return resp_maker.make_response(err)

    @app.route("/factor/<factor>/version", methods=['POST'])
    @ServiceDebugger.debug()
    def create_version(factor):
        """
        create a new factor version
        :param factor:
        :return: return message
        """
        # TODO: validate input
        code_file = request.files['code'].read()
        version = request.form.get("version")

        err = name_node.create_version(factor, version, code_file)
        return resp_maker.make_response(err)

    @app.route("/group_factor/version", methods=['POST'])
    @ServiceDebugger.debug()
    def create_group_factor_version():
        """
        create a new factor group version
        :return: return message
        """
        import json

        try:
            code_file = request.files['code'].read()
            factors = request.form.get("factors")
            factors = json.loads(factors)
            version = request.form.get("version")
        except:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err = name_node.create_group_factor_version(factors, version, code_file)
        return resp_maker.make_response(err)

    @app.route("/factor/<factor>/version", methods=['GET'])
    @ServiceDebugger.debug()
    def list_versions(factor):
        """
        list all versions of a factor/group factor
        :param factor:
        :return: return message
        """
        err, versions = name_node.list_versions(factor)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, str(versions))

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>", methods=['POST'])
    @ServiceDebugger.debug()
    def create_factor_stock_linkage(factor, version, stock_code):
        """
        create a new linkage between a specified factor version and a stock code
        :param factor:
        :param version:
        :param stock_code:
        :return: return message
        """
        # TODO: validate input
        ret_code = name_node.create_stock_linkage(factor, stock_code, version=version)
        return resp_maker.make_response(ret_code)

    @app.route("/factor/<factor>/stock/<stock_code>", methods=['POST'])
    @ServiceDebugger.debug()
    def create_latest_factor_stock_linkage(factor, stock_code):
        """
        create a new linkage between the latest version of a factor and a stock code
        :param factor:
        :param stock_code:
        :return: return message
        """
        # TODO: validate input
        ret_code = name_node.create_stock_linkage(factor, stock_code)
        return resp_maker.make_response(ret_code)

    @app.route("/factor/<factor>/version/<version>/stock", methods=['GET'])
    @ServiceDebugger.debug()
    def list_linked_stocks(factor, version):
        """
        list all linked stocks
        :param factor:
        :param version:
        :return: return message
        """
        err, stocks = name_node.list_linked_stocks(factor, version)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, str(stocks))

    @app.route("/factor/<factor>/stock", methods=['GET'])
    @ServiceDebugger.debug()
    def list_latest_factor_linked_stocks(factor):
        """
        list all linked stocks of the latest version of a factor
        :param factor:
        :return: return message
        """
        err, stocks = name_node.list_linked_stocks(factor, None)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, str(stocks))

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>", methods=['GET'])
    @ServiceDebugger.debug()
    def list_linkage_status(factor, version, stock_code):
        """
        list linkage status
        :param factor:
        :param version:
        :param stock_code:
        :return: return message
        """
        err, status = name_node.get_linkage_status(factor, version, stock_code)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, status)

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>", methods=['PUT'])
    @ServiceDebugger.debug()
    def update_factor_result(factor, version, stock_code):
        """
        update factor data
        :param factor:
        :param version:
        :param stock_code:
        :return: return message
        """

        err, status = name_node.update_factor_result(factor, stock_code, version=version)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, status)

    @app.route("/factor/<factor>/stock/<stock_code>", methods=['PUT'])
    @ServiceDebugger.debug()
    def update_latest_factor_result(factor, stock_code):
        """
        update factor data of the latest version
        :param factor:
        :param stock_code:
        :return: return message
        """

        err, status = name_node.update_factor_result(factor, stock_code)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, status)

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>/date/<fetch_date>", methods=['GET'])
    @ServiceDebugger.debug()
    def load_factor_result(factor, version, stock_code, fetch_date):
        """
        fetch factor data
        :param factor:
        :param version:
        :param stock_code:
        :param fetch_date:
        :return: return message
        """
        # TODO: validate input
        fetch_date = datetime.datetime.strptime(fetch_date, "%Y-%m-%d")
        err, df = name_node.load_factor_results(factor, stock_code, fetch_date, version=version)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, df.to_json())

    @app.route("/factor/load_multi_factors", methods=['POST'])
    @ServiceDebugger.debug()
    def load_multi_factors_result():
        """
        fetch factor data of multiple factors
        :return: return message
        """
        import json

        try:
            factors_json = request.form.get("factors")
            factors = json.loads(factors_json)
            stock_code = request.form.get("stock_code")
            fetch_date = request.form.get("fetch_date")
            fetch_date = datetime.datetime.strptime(fetch_date, "%Y-%m-%d")
        except:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err, res_df = name_node.load_multi_factor_results(factors, stock_code, fetch_date)
        if err:
            return resp_maker.make_response(err, res_df)
        else:
            return resp_maker.make_response(err, res_df.to_json())

    @app.route("/factor/load_multi_factors_by_range", methods=['POST'])
    @ServiceDebugger.debug()
    def load_multi_factors_result_by_range():
        """
        fetch factor data of multiple factors by time range
        :return: return message
        """
        import json

        try:
            factors_json = request.form.get("factors")
            factors = json.loads(factors_json)
            stock_code = request.form.get("stock_code")
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        except:
            return resp_maker.make_response(Error.ERROR_PARAMETER_MISSING_OR_INVALID)

        err, res_df = name_node.load_multi_factor_result_by_range(factors, stock_code, start_date, end_date)
        if err:
            return resp_maker.make_response(err, res_df)
        else:
            return resp_maker.make_response(err, res_df.to_json())

    @app.route("/factor/<factor>/stock/<stock_code>/date/<fetch_date>", methods=['GET'])
    @ServiceDebugger.debug()
    def load_latest_factor_result(factor, stock_code, fetch_date):
        """
        fetch factor data of the latest version of a factor
        :param factor:
        :param stock_code:
        :param fetch_date:
        :return: return message
        """
        # TODO: validate input
        fetch_date = datetime.datetime.strptime(fetch_date, "%Y-%m-%d")
        err, df = name_node.load_factor_results(factor, stock_code, fetch_date)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, df.to_json())

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>/update_status", methods=['GET'])
    @ServiceDebugger.debug()
    def get_factor_status(factor, version, stock_code):
        """
        get factor status
        :param factor:
        :param version:
        :param stock_code:
        :return: return message
        """
        err, status = name_node.get_factor_update_status(factor, stock_code, version)

        if err:
            return resp_maker.make_response(err)
        else:
            status_string = "total tasks:{0} finished_tasks:{1} finish_ratio:{2}\n last_updated_date:{3}". \
                format(status['total_tasks'], status['progress'], status['finish_ratio'], status['last_updated_date'])
            return resp_maker.make_response(err, status_string)

    @app.route("/factor/<factor>/stock/<stock_code>/update_status", methods=['GET'])
    @ServiceDebugger.debug()
    def get_latest_factor_status(factor, stock_code):
        """
        get factor status of the latest version
        :param factor:
        :param stock_code:
        :return: return message
        """
        err, status = name_node.get_factor_update_status(factor, stock_code)

        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, status)

    @app.route("/factor/<factor>/version/<version>/stock/<stock_code>/date", methods=['GET'])
    @ServiceDebugger.debug()
    def get_factor_date_list(factor, version, stock_code):
        """
        get updated factor dates list
        :param factor:
        :param version:
        :param stock_code:
        :return: return message
        """
        err, date_list = name_node.list_updated_dates(factor, stock_code, version)

        if err:
            return resp_maker.make_response(err)
        else:
            dates_string =str([str(day) for day in date_list])
            return resp_maker.make_response(err, dates_string)

    @app.route("/factor/<factor>/stock/<stock_code>/date", methods=['GET'])
    @ServiceDebugger.debug()
    def get_latest_factor_date_list(factor, stock_code):
        """
        get updated factor dates list of the latest version
        :param factor:
        :param stock_code:
        :return: return message
        """
        err, date_list = name_node.list_updated_dates(factor, stock_code)

        if err:
            return resp_maker.make_response(err)
        else:
            dates_string = str([str(day) for day in date_list])
            return resp_maker.make_response(err, dates_string)

    @app.route("/manager/stop_all", methods=['POST'])
    @ServiceDebugger.debug()
    def stop_update_process():
        """
        stop all tasks
        :return: return message
        """
        err = name_node.stop_all_tasks()
        return resp_maker.make_response(err)


def define_service(app, name_node):
    define_name_node_service(app, name_node)