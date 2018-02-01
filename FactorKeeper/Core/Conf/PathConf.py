import os


class Path(object):
    # root path
    SKYECON_BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

    # factor path
    FACTOR_GENERATOR_BASE = "{}/FactorGenerators".format(SKYECON_BASE)
    FACTOR_GENERATOR_MODULE_BASE = "FactorGenerators"
    FACTOR_GENERATOR_MODULE_NAME = "FactorGenerator"
    FACTOR_GENERATOR_MAIN_FILE_NAME = "factor_generator"
    FACTOR_GENERATOR_ZIP_TEMP_NAME = "factor_generator.factor_keeper.temp.zip"
    FACTOR_GENERATOR_UNZIP_DIR_NAME = "factor_generator.factor_keeper.temp"

    # Log Path, Relative to Bin Dir
    WORKERNODE_MANAGER_LOG_PATH = "../Log/WorkerNode/Manager"
    WORKERNODE_WORKER_LOG_PATH = "../Log/WorkerNode/Workers"
    NAMENODE_MANAGER_LOG_PATH = "../Log/NameNode"

    @staticmethod
    def make_factor_generator_path(factor_id, factor_version):
        return "{0}/{1}/{2}.py".format(Path.FACTOR_GENERATOR_BASE, factor_id, factor_version)

    @staticmethod
    def make_factor_generator_module_path(factor_id, factor_version):
        return "{0}.{1}.{2}.{3}".format(Path.FACTOR_GENERATOR_MODULE_BASE, factor_id,
                                        factor_version, Path.FACTOR_GENERATOR_MODULE_NAME)