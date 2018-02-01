"""
    This file defines loggers used in factor keeper.
    You should not modify this file.
"""


import datetime
from threading import Lock


class Logger(object):
    """
        A simple logger which cannot be used in multi-thread or multi-process environment.
    """
    def __init__(self, log_dir, log_stack):
        self.log_dir = log_dir
        self.log_stack = log_stack

        self.error_file = None
        self.warn_file = None
        self.info_file = None
        self.last_log_time = None

        self._reset_log_file()

    def sub_logger(self, sub_module_name):
        """
        Add sub module name to log stack
        :param sub_module_name:
        :return: a new logger which shares log file descriptor with its parent logger
        """
        return LoggerProxy(self, "/".join([self.log_stack, sub_module_name]))

    def _reset_log_file(self):
        now = datetime.datetime.now()
        self.last_log_time = now
        now_str = now.strftime("%Y-%m-%d_%H-%M")

        self.error_path = "{0}/error/error_{1}.log".format(self.log_dir, now_str)
        self.warn_path = "{0}/warn/warn_{1}.log".format(self.log_dir, now_str)
        self.info_path = "{0}/info/info_{1}.log".format(self.log_dir, now_str)

        if self.error_file is not None:
            self.error_file.close()

        if self.warn_file is not None:
            self.warn_file.close()

        if self.info_file is not None:
            self.info_file.close()

        self.error_file = open(self.error_path, mode="w", encoding="utf-8")
        self.warn_file = open(self.warn_path, mode="w", encoding="utf-8")
        self.info_file = open(self.info_path, mode="w", encoding="utf-8")

    def _new_log(self, content, now, log_stack=None):
        return "\n<" + ("=" * 10) + ">\n" + \
               "log_stack:{}\n".format(log_stack or self.log_stack) + \
               "time:{}\n".format(now) + \
               content

    def log(self, content, level="ERROR", log_stack=None):
        try:
            now = datetime.datetime.now()
            if now - self.last_log_time > datetime.timedelta(days=0.5):
                self._reset_log_file()

            if level == "ERROR":
                self.error_file.write(self._new_log(content, now, log_stack=log_stack))
                self.error_file.flush()
            elif level == "WARN":
                self.warn_file.write(self._new_log(content, now, log_stack=log_stack))
                self.warn_file.flush()
            else:
                self.info_file.write(self._new_log(content, now, log_stack=log_stack))
                self.info_file.flush()
        except Exception as e:
            print(e)

    def log_error(self, content, log_stack=None):
        self.log(content, "ERROR", log_stack=log_stack)

    def log_warn(self, content, log_stack=None):
        self.log(content, "WARN", log_stack=log_stack)

    def log_info(self, content, log_stack=None):
        self.log(content, "INFO", log_stack=log_stack)


class TSLogger(object):
    """
        A thread-safe logger implementation.
    """
    def __init__(self, log_dir, log_stack):
        self._logger = Logger(log_dir, log_stack)
        self.lock = Lock()

    def log_error(self, content, log_stack=None):
        self.lock.acquire()
        self._logger.log_error(content, log_stack=log_stack)
        self.lock.release()

    def log_warn(self, content, log_stack=None):
        self.lock.acquire()
        self._logger.log_warn(content, log_stack=log_stack)
        self.lock.release()

    def log_info(self, content, log_stack=None):
        self.lock.acquire()
        self._logger.log_info(content, log_stack=log_stack)
        self.lock.release()

    def sub_logger(self, sub_module_name):
        print("TSSublogger: " + "/".join([self._logger.log_stack, sub_module_name]))
        return LoggerProxy(self, "/".join([self._logger.log_stack, sub_module_name]))


class LoggerProxy(object):
    """
        A logger proxy used to share file descriptor between multiple loggers
    """
    def __init__(self, logger, log_stack):
        self.logger = logger
        self.log_stack = log_stack

    def log_error(self, content, log_stack=None):
        self.logger.log_error(content, log_stack=(log_stack or self.log_stack))

    def log_warn(self, content, log_stack=None):
        self.logger.log_warn(content, log_stack=(log_stack or self.log_stack))

    def log_info(self, content, log_stack=None):
        self.logger.log_info(content, log_stack=(log_stack or self.log_stack))

    def sub_logger(self, sub_module_name):
        return LoggerProxy(self.logger, "/".join([self.log_stack, sub_module_name]))
