from Core.WorkerNode.WorkerNodeImpl.MessageSender import MessageSender
from Core.Conf.WorkerConf import WorkerConf
from Core.Error.Error import Error
from Version import WorkerNodeVersion
from Core.Conf.MasterConf import MasterConf
import traceback


class Initializer(object):
    def __init__(self, logger):
        self.logger = logger.sub_logger(self.__class__.__name__)

    def register_worker(self, retry=False):
        err, min_version = MessageSender.register_worker(WorkerConf.SERVER_HOST, WorkerConf.SERVER_PORT,
                                                         WorkerConf.PROCESSOR_NUM, str(WorkerNodeVersion), self.logger)
        if err == Error.ERROR_WORKER_VERSION_DEPRECATED:
            err_version_too_old = "worker node version is too old({0}), min version({1}) is needed for name node".\
                format(WorkerNodeVersion, min_version)
            print(err_version_too_old)
            self.logger.log_error(err_version_too_old)
            exit(-1)
        elif err == Error.ERROR_HTTP_CONNECTION_FAILED:
            err_cannot_connect_to_namenode = "can not connect to name node. exception occurred during registration."
            print(err_cannot_connect_to_namenode)
            self.logger.log_error(err_cannot_connect_to_namenode)
            exit(-1)
        elif err:
            err_unexpected_exception = "an unexpected problem occurred. error code:{}".format(err)
            print(err_unexpected_exception)
            self.logger.log_error(err_unexpected_exception)
            if not retry:
                exit(-1)
            else:
                return Error.ERROR_CANNOT_CONNECT_TO_NAMENODE

        self.logger.log_info("successfully connect to namenode:{0}:{1}".format(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT))

        return Error.SUCCESS

    def init_worker_node(self):
        self.logger.log_info("initializing worker node...")
        self.register_worker()
        self.logger.log_info("successfully initialized worker node.")


class LogInitializer(object):
    @staticmethod
    def init_log_dir():
        import os
        assert os.path.exists("../Log")

        try:
            if not os.path.exists("../Log/WorkerNode"):
                os.mkdir("../Log/WorkerNode")
            if not os.path.exists("../Log/WorkerNode/Workers"):
                os.mkdir("../Log/WorkerNode/Workers")
            if not os.path.exists("../Log/WorkerNode/Manager"):
                os.mkdir("../Log/WorkerNode/Manager")

            log_dirs = ['error', 'warn', 'info']
            for ld in log_dirs:
                if not os.path.exists("../Log/WorkerNode/Workers/" + ld):
                    os.mkdir("../Log/WorkerNode/Workers/" + ld)
            for ld in log_dirs:
                if not os.path.exists("../Log/WorkerNode/Manager/" + ld):
                    os.mkdir("../Log/WorkerNode/Manager/" + ld)
        except:
            traceback.print_exc()
            exit()