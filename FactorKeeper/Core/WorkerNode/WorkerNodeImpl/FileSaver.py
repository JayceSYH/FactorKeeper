from Core.Error.Error import Error
from Core.Conf.PathConf import Path
import os, shutil, traceback
from Util.ZipUtil.ZipUtil import ZipUtil


class FileSaver(object):
    @staticmethod
    def save_code_to_fs(factor_id, factor_version, code_file, logger):
        """
        save factor generate code file to local file system
        :param logger:
        :param factor_id:
        :param factor_version:
        :param code_file:
        :return: err_code
        """

        # predefine file names
        factor_dir = "{0}/{1}".format(Path.FACTOR_GENERATOR_BASE, factor_id)
        factor_version_dir = "{0}/{1}/{2}".format(Path.FACTOR_GENERATOR_BASE, factor_id, factor_version)
        temp_file_path = "{0}/{1}".format(factor_version_dir, Path.FACTOR_GENERATOR_ZIP_TEMP_NAME)
        unzip_path = "{0}/{1}".format(factor_version_dir, Path.FACTOR_GENERATOR_UNZIP_DIR_NAME)

        # try to create factor version directory and put code in it
        try:
            # create factor directory
            if not os.path.exists(factor_dir):
                os.mkdir(factor_dir)
                with open("{}/__init__.py".format(factor_dir), 'w') as f:
                    pass

            # create factor version directory
            os.mkdir(factor_version_dir)
            with open("{}/__init__.py".format(factor_version_dir), 'w'):
                pass

            # save zip file as temp file
            with open(temp_file_path, 'wb') as temp_f:
                temp_f.write(code_file)

            # unzip temp file
            ZipUtil.unzip_file(temp_file_path, unzip_path)

            # remove temp zip file
            os.remove(temp_file_path)

            # name of code file or directory
            fg_name = os.listdir(unzip_path)[0]

            # move code to factor version directory from temp directory
            shutil.move("{0}/{1}".format(unzip_path, fg_name),
                        "{0}/{1}".format(factor_version_dir, fg_name))

            # remove unzip temp file
            os.rmdir(unzip_path)

            return Error.SUCCESS

        except Exception as e:
            logger.log_error(traceback.format_exc())

            # try to remove factor version directory
            try:
                for root, dirs, files in os.walk(factor_version_dir, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
            except:
                pass

            return Error.ERROR_FILE_UPLOAD_FAILED
