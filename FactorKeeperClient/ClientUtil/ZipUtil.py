import os, zipfile


class ZipUtil(object):
    @staticmethod
    def zip_dir(dir_path, target_path):
        zipf = zipfile.ZipFile(target_path, 'w')
        pre_len = len(os.path.dirname(dir_path))
        for parent, dirnames, filenames, in os.walk(dir_path):
            for filename in filenames:
                pathfile = os.path.join(parent, filename)
                arcname = pathfile[pre_len:].strip(os.path.sep)
                zipf.write(pathfile, arcname)
        zipf.close()

    @staticmethod
    def zip_file(file_path, target_path):
        f = zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED)
        f.write(file_path, os.path.basename(file_path), zipfile.ZIP_DEFLATED)
        f.close()

    @staticmethod
    def zip(source_path, dest_path):
        if os.path.isdir(source_path):
            ZipUtil.zip_dir(source_path, dest_path)
        else:
            ZipUtil.zip_file(source_path, dest_path)

    @staticmethod
    def unzip_file(file_path, target_path):
        f = zipfile.ZipFile(file_path, 'r')
        f.extractall(target_path)