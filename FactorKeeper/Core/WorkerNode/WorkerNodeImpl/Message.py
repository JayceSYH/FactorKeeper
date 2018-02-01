class Message(object):
    def __init__(self, target, target_type, task_id, message_type, content=""):
        self.target = target
        self.type = message_type
        self.content = content
        self.task_id = task_id
        self.target_type = target_type


class ProcessMetricMessage(Message):
    def __init__(self, task_id, progress):
        super(ProcessMetricMessage, self).__init__(MessageConst.MessageTarget.TARGET_TASK_MANAGER,
                                                   MessageConst.MessageTargetType.TARGET_TYPE_MANAGER,
                                                   task_id,
                                                   MessageConst.MessageType.PROGRESS_METRIC, "{}".format(progress))


class FinishACKMessage(Message):
    def __init__(self, task_id, aborted=False):
        super(FinishACKMessage, self).__init__(MessageConst.MessageTarget.TARGET_TASK_MANAGER,
                                               MessageConst.MessageTargetType.TARGET_TYPE_MANAGER,
                                               task_id,
                                               MessageConst.MessageType.FINISH)

        self.aborted = aborted


class LogMessage(Message):
    def __init__(self, task_id, log_content, log_level, log_stack=""):
        super(LogMessage, self).__init__(MessageConst.MessageTarget.TARGET_TASK_MANAGER,
                                         MessageConst.MessageTargetType.TARGET_TYPE_MANAGER,
                                         task_id,
                                         MessageConst.MessageType.LOG)
        self.log_level = log_level
        self.log_content = log_content
        self.log_stack = log_stack

    @staticmethod
    def error(task_id, content, log_stack=""):
        return LogMessage(task_id, content, log_level=MessageConst.MessageLogLevel.ERROR, log_stack=log_stack)

    @staticmethod
    def warn(task_id, content, log_stack=""):
        return LogMessage(task_id, content, log_level=MessageConst.MessageLogLevel.WARN, log_stack=log_stack)

    @staticmethod
    def info(task_id, content, log_stack=""):
        return LogMessage(task_id, content, log_level=MessageConst.MessageLogLevel.INFO, log_stack=log_stack)


class KillMessage(Message):
    def __init__(self, task_id):
        super(KillMessage, self).__init__(MessageConst.MessageTarget.TARGET_TASK_MANAGER,
                                               MessageConst.MessageTargetType.TARGET_TYPE_MANAGER,
                                               task_id,
                                               MessageConst.MessageType.KILL)


class MessageLogger(object):
    def __init__(self, task_id, task_group_id, log_stack, task_queue):
        self.task_id = task_id
        self.log_stack = "{0}/{1}/{2}".format(log_stack, task_group_id, task_id)
        self.task_queue = task_queue

    def log_error(self, content, log_stack=None):
        self.task_queue.put(LogMessage.error(self.task_id, content, log_stack=log_stack or self.log_stack))

    def log_warn(self, content, log_stack=None):
        self.task_queue.put(LogMessage.error(self.task_id, content, log_stack=log_stack or self.log_stack))

    def log_info(self, content, log_stack=None):
        self.task_queue.put(LogMessage.info(self.task_id, content, log_stack=log_stack or self.log_stack))

    def sub_logger(self, sub_module_name):
        from Core.Logger.Logger import LoggerProxy
        return LoggerProxy(self, "/".join([self.log_stack, sub_module_name]))


class MessageConst(object):
    class MessageTarget(object):
        TARGET_TASK_MANAGER = "task_manager"

    class MessageTargetType(object):
        TARGET_TYPE_TASK = "TASK"
        TARGET_TYPE_GROUP = "GROUP"
        TARGET_TYPE_MANAGER = "MANAGER"

    class MessageType(object):
        PROGRESS_METRIC = 1
        USER_DEFINE = 2
        FINISH = 3
        LOG = 4
        KILL = 5

    class MessageLogLevel(object):
        ERROR = "ERROR"
        WARN = "WARN"
        INFO = "INFO"
