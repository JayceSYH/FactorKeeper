"""
    As a result of sharing database engine between multiple thread, "threaded" must be set to True
    instead of using multi-processing mode.
    You should not modify this file.
"""


from Core.WorkerNode.CGI.Service import define_service
from Core.Conf.WorkerConf import WorkerConf


class CGIRunner(object):
    def __init__(self, worker_node):
        from flask import Flask

        self.__app = Flask("WorkerNode")
        define_service(self.__app, worker_node)

    def run(self, threaded=True):
        self.__app.run(WorkerConf.SERVER_HOST, WorkerConf.SERVER_PORT, threaded=True)
