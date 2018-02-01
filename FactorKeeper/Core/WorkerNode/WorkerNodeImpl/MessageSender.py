from Core.Conf.MasterConf import MasterConf
from Core.Conf.ProtoConf import ProtoConf
from Core.Error.Error import Error
import requests, datetime, traceback


class MessageSender(object):
    """
        MessageSend is used to send messages to master node, usually used in callbacks
    """
    @staticmethod
    def _get_result(response):
        response = response
        print(response)
        first_space = response.find(" ")
        ret_code = response[:first_space]
        if len(response) != first_space + 1:
            ret_msg = response[first_space + 1:]
        else:
            ret_msg = ""

        return int(ret_code), ret_msg

    @staticmethod
    def send_factor_result_to_master(factor, version, stock_code, day, df, task_id, logger):
        url = "http://{0}:{1}/worker/call_back/update_factor/update".format(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT)

        try:
            resp = requests.post(url, data={
                "HEADER": ProtoConf.CALLBACK_HEADER,
                "factor": factor,
                "version": version,
                "stock_code": stock_code,
                "date": day,
                "data_frame": df.to_json(),
                "task_id": task_id
            }).text
        except:
            logger.log_error(traceback.format_exc())
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            err, msg = MessageSender._get_result(resp)
            return err, msg

        else:
            logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_SERVER_INTERNAL_ERROR, None

    @staticmethod
    def send_tick_data_result_to_master(stock_code, day, df, task_id, logger):
        url = "http://{0}:{1}/worker/call_back/update_tick_data/update".format(MasterConf.SERVER_HOST,
                                                                               MasterConf.SERVER_PORT)

        try:
            resp = requests.post(url, data={
                "HEADER": ProtoConf.CALLBACK_HEADER,
                "stock_code": stock_code,
                "date": day,
                "data_frame": df.to_json(),
                "task_id": task_id
            }).text
        except:
            logger.log_error(traceback.format_exc())
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            err, msg = MessageSender._get_result(resp)
            return err, msg

        else:
            logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_SERVER_INTERNAL_ERROR, None

    @staticmethod
    def send_finish_ack(task_id, task_status, logger):
        url = "http://{0}:{1}/worker/call_back/finish".format(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT)
        data = {
            "HEADER": ProtoConf.CALLBACK_HEADER,
            "task_id": task_id
        }
        data.update(task_status)

        try:
            resp = requests.post(url, data).text
        except:
            logger.log_error(traceback.format_exc())
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            err, msg = MessageSender._get_result(resp)
            return err, msg

        else:
            logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_SERVER_INTERNAL_ERROR, None

    @staticmethod
    def register_worker(host, port, cores, version, logger):
        url = "http://{0}:{1}/worker".format(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT)

        try:
            resp = requests.post(url, data={
                "HEADER": ProtoConf.WORKER_HEADER,
                "host": host,
                "port": port,
                "cores": cores,
                "version": version
            }).text
        except:
            logger.log_error(traceback.format_exc())
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            err, msg = MessageSender._get_result(resp)
            return err, msg

        else:
            logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

    @staticmethod
    def update_worker_info(host, port, tasks, logger):
        url = "http://{0}:{1}/worker".format(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT)

        try:
            resp = requests.put(url, data={
                "HEADER": ProtoConf.WORKER_HEADER,
                "host": host,
                "port": port,
                "tasks": ProtoConf.TASK_SPLITTER.join(tasks),
                "update_time": str(datetime.datetime.now().strftime(ProtoConf.DATETIME_FORMAT))
            }).text
        except:
            logger.log_error(traceback.format_exc())
            return Error.ERROR_HTTP_CONNECTION_FAILED, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            err, msg = MessageSender._get_result(resp)
            return err, msg

        else:
            logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_SERVER_INTERNAL_ERROR, None
