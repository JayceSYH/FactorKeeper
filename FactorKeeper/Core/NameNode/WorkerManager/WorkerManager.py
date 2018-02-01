from Core.Error.Error import Error
from Core.Conf.MasterConf import MasterConf
from Core.Conf.ProtoConf import ProtoConf
from Version import MinWorkerNodeVersion, Version
import threading, datetime, requests


class WorkerManager(object):
    """
        Worker manager is used to manage all registered workers so other modules don't need to
        care about choosing a worker to execute a task
    """
    def __init__(self, logger):
        self.workers = {}
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.lock = threading.Lock()

    @staticmethod
    def _check_worker_version(worker_version):
        return Version.strpversion(worker_version) >= MinWorkerNodeVersion

    def _check_worker_status(self, host, port):
        worker_addr = self._worker_addr(host, port)
        now = datetime.datetime.now()
        receive_time = now

        self.lock.acquire()
        try:
            is_exists = worker_addr in self.workers
            if is_exists:
                worker = self.workers[worker_addr]
                receive_time = worker.receive_time
        finally:
            self.lock.release()

        if is_exists:
            if now - receive_time > datetime.timedelta(seconds=MasterConf.WORKER_ACK_TIME_OUT):
                self.remove_worker(host, port)
                return False  # disconnected
            else:
                return True  # alive
        else:
            return False  # not exists

    @staticmethod
    def _worker_addr(host, port):
        return "{0}:{1}".format(host, port)

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

    def _send_command(self, url, method, main_data, params):
        data = {"HEADER": ProtoConf.COMMAND_HEADER}
        data.update(main_data)
        if method == "POST":
            resp = requests.post(url, data=data).text
        elif method == "GET":
            resp = requests.get(url, params=params).text
        elif method == "PUT":
            resp = requests.put(url, data=data).text
        elif method == "DELETE":
            resp = requests.delete(url).text
        else:
            return Error.ERROR_UNSUPPORTED_HTTP_METHOD, None

        if resp.startswith(ProtoConf.RET_MSG_HEADER):
            resp = resp[len(ProtoConf.RET_MSG_HEADER):]
            ret_code, ret_msg = self._get_result(resp)
            return ret_code, ret_msg
        else:
            self.logger.log_error("Unrecognized return message:\n" + resp)
            return Error.ERROR_SERVER_INTERNAL_ERROR, None

    def register_worker(self, host, port, cores, worker_version):
        """
        register a new worker
        :param host:
        :param port:
        :param cores:
        :param worker_version:
        :return: err_code, error message
        """
        if self._check_worker_version(worker_version):
            worker_addr = self._worker_addr(host, port)

            self.lock.acquire()
            try:
                if worker_addr in self.workers:
                    self.logger.log_info("worker({}) restarted. all relative tasks will be reset")
                self.workers[worker_addr] = WorkerInfo(host, port, cores)
            finally:
                self.lock.release()

            self.logger.log_info("worker({}) connected".format(worker_addr))
            return Error.SUCCESS, None
        else:
            self.logger.log_warn("an old version worker({0}) node was trying to register, but a newer version({1}) is "
                                 "needed".format(worker_version, MinWorkerNodeVersion))
            return Error.ERROR_WORKER_VERSION_DEPRECATED, str(MinWorkerNodeVersion)

    def remove_worker(self, host, port):
        """
        remove a deprecated worker
        :param host:
        :param port:
        :return: err_code
        """
        worker_addr = self._worker_addr(host, port)
        if worker_addr in self.workers:

            self.lock.acquire()
            try:
                if worker_addr in self.workers:
                    del self.workers[worker_addr]
            finally:
                self.lock.release()

            self.logger.log_info("worker({}) disconnected".format(worker_addr))

        return Error.SUCCESS

    def update_worker(self, host, port, tasks, update_time):
        """
        update worker status
        :param host:
        :param port:
        :param tasks:
        :param update_time:
        :return: err_code
        """
        worker_addr = self._worker_addr(host, port)

        self.lock.acquire()
        try:
            is_exists = worker_addr in self.workers
            if is_exists:
                self.workers[worker_addr].update(tasks, update_time)
        finally:
            self.lock.release()

        if is_exists:
            self.logger.log_info("worker({}) updated".format(worker_addr))
            return Error.SUCCESS
        else:
            self.logger.log_warn("update not registered worker")
            return Error.ERROR_WORKER_NOT_EXISTS

    def list_workers(self):
        """
        list all workers
        :return: err_code, list of workers
        """
        self.lock.acquire()
        try:
            worker_list = [self.workers[addr] for addr in self.workers]
        finally:
            self.lock.release()

        worker_list = [worker for worker in worker_list if self.is_alive(worker)]
        return Error.SUCCESS, worker_list

    def is_workers_ready(self):
        """
        Check if there're available workers
        :return: err_code, True if workers are available else False
        """
        self.lock.acquire()
        try:
            is_workers_ready = len(self.workers) > 0
        finally:
            self.lock.release()
        return Error.SUCCESS, is_workers_ready

    def is_alive(self, old_worker, ret_worker=False):
        """
        Check weather a worker is alive
        :param old_worker:
        :param ret_worker:
        :return: err_code, True if alive else false
        """
        self.lock.acquire()
        try:
            worker = self.workers.get(self._worker_addr(old_worker.host, old_worker.port), None)
        finally:
            self.lock.release()

        if worker is None or worker.id != old_worker.id:
            return False
        else:
            if ret_worker:
                return self._check_worker_status(worker.host, worker.port), worker
            else:
                return self._check_worker_status(worker.host, worker.port)

    def send_command(self, command, method='POST', worker=None, data="", param=None, broadcast=False):
        """
        send command to a low burden worker or a specified worker
        :param command: command(a target url)
        :param method: one of HTTP methods
        :param worker: a specified worker to send command, if none, an relative low burden worker will be assigned
        :param data: usually a python dict, used when method is "POST" or "PUT"
        :param param: query param, used when method is "GET"
        :param broadcast: broadcast to all workers. eg: stop all tasks
        :return: err_code, err_msg
        """
        if param is None:
            param = {}
        url_template = "http://{0}:{1}/" + command

        if broadcast:
            self.lock.acquire()
            try:
                worker_list = [self.workers[addr] for addr in self.workers]
            finally:
                self.lock.release()

            for worker in worker_list:
                self._send_command(url_template.format(worker.host, worker.port), method, data, param)

            return Error.SUCCESS
        else:
            if worker is None:
                if len(self.workers) == 0:
                    return Error.ERROR_NO_WORKER_TO_BE_ASSIGNED, None, None

                for try_count in range(3):
                    self.lock.acquire()
                    try:
                        candidates = [self.workers[addr] for addr in self.workers]
                    finally:
                        self.lock.release()

                    candidates.sort(key=lambda candidate: float(len(candidate.tasks)) / candidate.cores)

                    for candidate in candidates:
                        if self._check_worker_status(candidate.host, candidate.port):
                            err, message = self._send_command(url_template.format(candidate.host, candidate.port), method,
                                                              data, param)
                            if err == Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE:
                                return err, message, None
                            if not err:
                                return err, message, candidate

                return Error.ERROR_FAILED_TO_SEND_TASK_COMMAND, None, None
            else:
                if self._check_worker_status(worker.host, worker.port):
                    err, message = self._send_command(url_template.format(worker.host, worker.port), method, data, param)
                    return err, message, worker
                else:
                    return Error.ERROR_FAILED_TO_SEND_TASK_COMMAND, None, None


class WorkerInfo(object):
    """
        Consist of a worker's status
    """
    def __init__(self, host, port, cores, alive=True):
        self.id = "{0}:{1}:{2}".format(host, port, datetime.datetime.now())
        self.host = host
        self.port = port
        self.cores = cores
        self.alive = alive
        self.tasks = []
        self.create_time = datetime.datetime.now()
        self.update_time = self.create_time
        self.receive_time = self.create_time
        self.lock = threading.Lock()

    def update(self, tasks, update_time):
        if update_time > self.update_time:
            self.lock.acquire()
            try:
                self.receive_time = datetime.datetime.now()
                self.update_time = update_time
                self.tasks = tasks
            finally:
                self.lock.release()

    def __str__(self):
        task_str = "<br>".join([
            ("<br>" + ("&nbsp" * 12) + "'{}'".format(task_id)) for task_id in self.tasks
        ])

        worker_str = "Host: {1} <br>" +\
                     "Port: {2} <br>" +\
                     "Cores: {3} <br>" +\
                     "Tasks: <br>" +\
                     "&nbsp" * 10 + "[" +\
                     "{4} <br>" +\
                     "&nbsp" * 10 + "]<br>" +\
                     "CreateTime: {5} <br>" +\
                     "LastUpdateTime: {6} <br>" +\
                     "LastInfoReceiveTime: {7}"

        worker_str = worker_str.format(self.id, self.host, self.port, self.cores, task_str,
                                       self.create_time.strftime(ProtoConf.DATETIME_FORMAT),
                                       self.update_time.strftime(ProtoConf.DATETIME_FORMAT),
                                       self.receive_time.strftime(ProtoConf.DATETIME_FORMAT))

        worker_str = worker_str.replace("<br>", "<br>|")
        worker_str = "<br>" + "_" * 100 + "<br>|" + worker_str + "<br>|" + "_" * 100

        return worker_str
