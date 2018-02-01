"""
    This file defines services of file keeper, you may modify it if needed.
"""


def define_worker_node_service(app, worker_node):
    from flask import request
    from Util.ServiceUtil.Response import ResponseMaker
    from Util.ServiceUtil.Debug import ServiceDebugger
    from Core.Conf.ProtoConf import ProtoConf
    from Core.Error.Error import Error

    resp_maker = ResponseMaker()
    ServiceDebugger.set_debug(True)

    @app.route("/update_factor", methods=['POST'])
    @ServiceDebugger.debug()
    def update_factor_result():
        """
        start update factor result task
        :return: return message
        """

        header = request.form.get("HEADER")
        if header != ProtoConf.COMMAND_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        factor = request.form.get("factor")
        version = request.form.get("version")
        stock_code = request.form.get("stock_code")
        task_id = request.form.get("task_id")

        err, update_item_num = worker_node.update_factor_result(factor, version, stock_code, task_id)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, "{} days of factors updating...".format(update_item_num))

    @app.route("/update_tick_data", methods=['POST'])
    @ServiceDebugger.debug()
    def update_tick_data_result():
        """
        start update tick data task
        :return: return message
        """

        header = request.form.get("HEADER")
        if header != ProtoConf.COMMAND_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        stock_code = request.form.get("stock_code")
        task_id = request.form.get("task_id")

        err, update_item_num = worker_node.update_tick_data_result(stock_code, task_id)
        if err:
            return resp_maker.make_response(err)
        else:
            return resp_maker.make_response(err, "{} days of tick data updating...".format(update_item_num))

    @app.route("/update_factor/status", methods=['POST'])
    @ServiceDebugger.debug()
    def get_factor_status():
        """
        get factor update status
        :return: return message
        """

        header = request.form.get("HEADER")
        if header != ProtoConf.COMMAND_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        task_id = request.form.get("task_id")

        err, status = worker_node.query_update_status(task_id)

        if err:
            return resp_maker.make_response(err)
        else:
            status_string = "total tasks:{0}  total finished:{1} finished ratio:{2}%" \
                            "\nfinished tasks:{3} aborted tasks:{4}". \
                format(status['total_tasks'], status['progress'], int(status['finish_ratio'] * 100),
                       status['finished_num'], status['aborted_num'])
            return resp_maker.make_response(err, status_string)

    @app.route("/update_tick_data/status", methods=['POST'])
    @ServiceDebugger.debug()
    def get_tick_data_status():
        """
        get tick data update status
        :return: return message
        """

        header = request.form.get("HEADER")
        if header != ProtoConf.COMMAND_HEADER:
            return resp_maker.make_response(Error.ERROR_UNRECOGNIZED_HEADER, "unrecognized header '{}'".format(header))

        task_id = request.form.get("task_id")

        err, status = worker_node.query_update_status(task_id)

        if err:
            return resp_maker.make_response(err)
        else:
            status_string = "total tasks:{0}  total finished:{1} finished ratio:{2}%" \
                            "\nfinished tasks:{3} aborted tasks:{4}". \
                format(status['total_tasks'], status['progress'], int(status['finish_ratio'] * 100),
                       status['finished_num'], status['aborted_num'])
            return resp_maker.make_response(err, status_string)

    @app.route("/stop_all", methods=['POST'])
    @ServiceDebugger.debug()
    def stop_all_process():
        """
        stop all process
        :return: return message
        """
        err = worker_node.stop_all_tasks()
        return resp_maker.make_response(err)

    @app.route("/update_factor/stop", methods=['POST'])
    @ServiceDebugger.debug()
    def stop_factor_update_task():
        """
        stop a single task
        :return: return message
        """
        task_id = request.form.get("task_id")

        err = worker_node.stop_task(task_id)
        return resp_maker.make_response(err)


def define_service(app, worker_node):
    define_worker_node_service(app, worker_node)