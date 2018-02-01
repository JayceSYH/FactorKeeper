"""
    As a result of sharing database engine between multiple thread, "threaded" must be set to True
    instead of using multi-processing mode.
    You should not modify this file.
"""

from Core.NameNode.CGI.Service import define_service
from Core.Conf.MasterConf import MasterConf


class CGIRunner(object):
    def __init__(self, name_node):
        from flask import Flask

        self.__name_node = name_node
        self.__app = Flask("Master")
        define_service(self.__app, name_node)

    def run(self, threaded=True):
        self.__app.run(MasterConf.SERVER_HOST, MasterConf.SERVER_PORT, threaded=True)
