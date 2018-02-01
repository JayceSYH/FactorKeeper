class MasterConf(object):
    # Worker Manager
    WORKER_ACK_TIME_OUT = 30  # in seconds

    # Task Manager
    TASK_CHECK_CYCLE = 10  # in seconds

    # Database Conf
    DB_POOL_SIZE = 3
    DB_MAX_OVERFLOW = 5

    # Server
    SERVER_HOST = "localhost"
    SERVER_PORT = 8910
