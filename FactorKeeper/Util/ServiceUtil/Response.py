from Core.Error.Error import Error
from Core.Conf.ProtoConf import ProtoConf


class ResponseMaker(object):
    def __init__(self):
        self.__err_desc = {}
        self._init_error_description()

    def _init_error_description(self):
        self.__err_desc = {0: "SUCCESS"}
        for prop in dir(Error):
            if prop.startswith("ERROR_"):
                desc_parts = prop.split("_")[1:]
                desc = "Error: " + " ".join([part.lower() for part in desc_parts])
                self.__err_desc[getattr(Error, prop)] = desc

    def describe_error_code(self, err):
        return self.__err_desc.get(err, "Undefined error type")

    def make_response(self, ret_code, ret_msg=None):
        if ret_msg is None:
            ret_msg = self.describe_error_code(ret_code)
        resp = "{0} {1}".format(ret_code, ret_msg)

        return ProtoConf.RET_MSG_HEADER + resp
