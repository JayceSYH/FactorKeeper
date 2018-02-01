import requests, os
import pandas as pd
import datetime
from ClientUtil.ZipUtil import ZipUtil


class FactorKeeperClient(object):
    def __init__(self):
        self.host = "10.0.2.24"
        # self.host = "localhost"
        self.port = 8910
        self.url = "http://{0}:{1}".format(self.host, self.port)

    def create_stock_view(self, stock_view_name, stock_view_relation):
        """
        添加一个stock view
        :param stock_view_name:
        :param stock_view_relation:
        :return:
        """
        import json

        res = self._do_post("{0}/stock_view".format(self.url, stock_view_name), datas={
            "stock_view_name": stock_view_name,
            "stock_view_relation": json.dumps(stock_view_relation)
        })
        self._show_result(res)
        return FactorKeeperClient._get_result(res)

    def create_factor(self, factor, local_path):
        """
        添加一个factor
        :param local_path:
        :param factor: factor名称
        :return: 返回码、返回消息
        """
        local_dir = os.path.dirname(os.path.realpath(local_path))
        temp_file_path = "{0}{1}{2}".format(local_dir, os.sep, "SkyEconTransferTemp.SkyEconTemp.zip")
        ZipUtil.zip(local_path, temp_file_path)
        try:
            with open(temp_file_path, 'rb') as f:
                code_file = f.read()
                res = self._do_post("{0}/factor".format(self.url, factor),datas={
                    "factor": factor
                }, files={"code": code_file})
                self._show_result(res)
                return self._get_result(res)
        except Exception as e:
            print(e)
            return -1, "Zip file process failed."
        finally:
            os.remove(temp_file_path)

    def create_group_factor(self, factors, local_path):
        import json
        local_dir = os.path.dirname(os.path.realpath(local_path))
        temp_file_path = "{0}{1}{2}".format(local_dir, os.sep, "SkyEconTransferTemp.SkyEconTemp.zip")
        ZipUtil.zip(local_path, temp_file_path)
        try:
            with open(temp_file_path, 'rb') as f:
                code_file = f.read()
                res = self._do_post("{0}/group_factor".format(self.url), datas={
                    "factors": json.dumps(factors),
                }, files={"code": code_file})
                self._show_result(res)
                return self._get_result(res)
        except Exception as e:
            print(e)
            return -1, "Zip file process failed."
        finally:
            os.remove(temp_file_path)

    def create_factor_version(self, factor_id, factor_version, local_path):
        """
        创建一个factor版本
        :param factor_id: factor名称
        :param factor_version: factor版本名称
        :param local_path: 本地因子生成脚本路径
        :return: 返回码、返回消息
        """
        local_dir = os.path.dirname(os.path.realpath(local_path))
        temp_file_path = "{0}{1}{2}".format(local_dir, os.sep, "SkyEconTransferTemp.SkyEconTemp.zip")
        ZipUtil.zip(local_path, temp_file_path)
        try:
            with open(temp_file_path, 'rb') as f:
                code_file = f.read()
                res = self._do_post("{0}/factor/{1}/version".format(self.url, factor_id), datas={
                    "version": factor_version
                }, files={"code": code_file})
                self._show_result(res)
                return self._get_result(res)
        except Exception as e:
            print(e)
            return -1, "Zip file process failed."
        finally:
            os.remove(temp_file_path)

    def create_group_factor_version(self, factors, factor_version, local_path):
        import json
        local_dir = os.path.dirname(os.path.realpath(local_path))
        temp_file_path = "{0}{1}{2}".format(local_dir, os.sep, "SkyEconTransferTemp.SkyEconTemp.zip")
        ZipUtil.zip(local_path, temp_file_path)
        try:
            with open(temp_file_path, 'rb') as f:
                code_file = f.read()
                res = self._do_post("{0}/group_factor/version".format(self.url), datas={
                    "factors": json.dumps(factors),
                    "version": factor_version
                }, files={"code": code_file})
                self._show_result(res)
                return self._get_result(res)
        except Exception as e:
            print(e)
            return -1, "Zip file process failed."
        finally:
            os.remove(temp_file_path)

    def create_linkage(self, factor_id, stock_code, factor_version=None):
        """
        将一个factor版本与某支股票进行关联
        :param factor_id: factor名称
        :param factor_version: factor版本名称
        :param stock_code: 股票代码
        :return: 返回码、返回消息
        """
        if factor_version is not None:
            res = self._do_post("{0}/factor/{1}/version/{2}/stock/{3}".format(self.url, factor_id, factor_version,
                                                                              stock_code), "")
        else:
            res = self._do_post(
                "{0}/factor/{1}/stock/{2}".format(self.url, factor_id, stock_code), "")
        self._show_result(res)
        return FactorKeeperClient._get_result(res)

    def load_factor_result(self, factor_id, stock_code, fetch_date, factor_version=None):
        """
        加载因子计算结果
        :param factor_id: factor名称
        :param factor_version: factor版本
        :param stock_code: 股票代码
        :param fetch_date: 时间范围
        :return: 返回码、返回消息
        """
        if factor_version is not None:
            res = self._do_get("{0}/factor/{1}/version/{2}/stock/{3}/date/{4}".format(self.url, factor_id, factor_version, stock_code,
                                                                         fetch_date))
        else:
            res = self._do_get(
                "{0}/factor/{1}/stock/{2}/date/{3}".format(self.url, factor_id, stock_code, fetch_date))
        ret_code, ret_msg = FactorKeeperClient._get_result(res)
        if ret_code:
            return int(ret_code), pd.DataFrame()

        return ret_code, pd.read_json(ret_msg).sort_values(by=['datetime'])

    def load_multi_factor_result(self, factors, stock_code, fetch_date):
        import json
        res = self._do_post(
            "{0}/factor/load_multi_factors".format(self.url), datas={
                "factors": json.dumps(factors),
                "stock_code": stock_code,
                "fetch_date": str(fetch_date)
            })
        ret_code, ret_msg = FactorKeeperClient._get_result(res)
        if ret_code:
            return int(ret_code), pd.DataFrame()

        return ret_code, pd.read_json(ret_msg).sort_values(by=['datetime'])

    def load_multi_factor_result_by_range(self, factors, stock_code, start_date, end_date):
        import json
        res = self._do_post(
            "{0}/factor/load_multi_factors_by_range".format(self.url), datas={
                "factors": json.dumps(factors),
                "stock_code": stock_code,
                "start_date": str(start_date),
                "end_date": str(end_date)
            })
        ret_code, ret_msg = FactorKeeperClient._get_result(res)
        print(ret_msg)
        if ret_code:
            return int(ret_code), pd.DataFrame()

        return ret_code, pd.read_json(ret_msg).sort_values(by=['datetime'])

    def update_factor_result(self, factor_id, stock_code, factor_version=None):
        """
        更新因子结果
        :param factor_id: factor名称
        :param factor_version: factor版本
        :param stock_code: 股票代码
        :return: 返回码、返回消息
        """
        if factor_version is not None:
            res = self._do_put("{0}/factor/{1}/version/{2}/stock/{3}".format(self.url, factor_id, factor_version, stock_code))
            self._show_result(res)
        else:
            res = self._do_put(
                "{0}/factor/{1}/stock/{2}".format(self.url, factor_id, stock_code))
            self._show_result(res)
        return self._get_result(res)

    def get_update_status(self, factor_id, stock_code, factor_version=None):
        if factor_version is not None:
            res = self._do_get("{0}/factor/{1}/version/{2}/stock/{3}/update_status".format(self.url, factor_id, factor_version, stock_code))
            self._show_result(res)
        else:
            res = self._do_get("{0}/factor/{1}/stock/{2}/update_status".format(self.url, factor_id, stock_code))
            self._show_result(res)
        return FactorKeeperClient._get_result(res)

    def get_date_list(self, factor_id, factor_version, stock_code):
        stock_code = stock_code.replace(".", "_")
        res = self._do_get("{0}/factor/{1}/{2}/stock/{3}/dates".format(self.url, factor_id, factor_version, stock_code))
        self._show_result(res)
        ret_code, ret_msg = FactorKeeperClient._get_result(res)
        if ret_code:
            return int(ret_code), []
        else:
            return int(ret_code), ret_msg.split(" ")

    def stop_update_process(self):
        res = self._do_post("{0}/manager/stop_all".format(self.url), "")
        self._show_result(res)
        ret_code, ret_msg = FactorKeeperClient._get_result(res)
        return ret_code, ret_msg

    @staticmethod
    def _do_post(url, datas, files=None):
        return requests.post(url, datas, files=files).text

    @staticmethod
    def _do_get(url, params=None):
        params = params if params is not None else {}
        return requests.get(url, params=params).text

    @staticmethod
    def _do_put(url):
        return requests.put(url).text

    @staticmethod
    def _get_result(response):

        if response.startswith("<SKYECON2_RET_MSG>"):
            response = response[len("<SKYECON2_RET_MSG>"):]
            first_space = response.find(" ")
            ret_code = response[:first_space]
            if len(response) != first_space + 1:
                ret_msg = response[first_space + 1:]
            else:
                ret_msg = ""

            return int(ret_code), ret_msg
        else:
            return -1, response

    @staticmethod
    def _show_result(response):
        ret_code, ret_msg = FactorKeeperClient._get_result(response)
        if not ret_msg:
            if ret_code == 0:
                print("RetCode:0 (Action Succeed)")
            else:
                print("RetCode:{}".format(ret_code))
        else:
            print("RetCode:{0} ({1})".format(ret_code, ret_msg))


def stock_view_example():
    """
    创建股票数据视图，以股票代码：列的形式定义，列可以是单个string也可以是一个包含多个string的list
    视图名后缀必须为.VIEW
    :return:
    """
    sc = FactorKeeperClient()

    sc.create_stock_view("index_view.VIEW", {
        "002466.SZ": "last",
        "002460.SZ": "last"
    })


def group_factor_example():
    """
    创建组因子， 更新脚本的返回值必须是datarame，并且与列名与因子名相同（顺序无所谓）
    :return:
    """
    sc = FactorKeeperClient()

    sc.create_group_factor(["buy_close_position", "short_close_position"], "gen_factor")
    sc.create_linkage("buy_close_position", "index_view.VIEW")

    sc.update_factor_result("buy_close_position", "index_view.VIEW")


def load_multi_factor_example():
    """
    同时load多个因子结果，格式为因子名：版本，版本为None则取最新版
    :return:
    """
    sc = FactorKeeperClient()

    code, df = sc.load_multi_factor_result({
        "buy_close_position": None,
        "short_close_position": None
    }, "index_view.VIEW", datetime.date(2017, 6, 7))
    print(code)
    df.info()


if __name__ == "__main__":
    # stock_view_example()
    #
    # group_factor_example()
    #
    # load_multi_factor_example()
    sc = FactorKeeperClient()
    code, df = sc.load_multi_factor_result_by_range({
        "cross_num": None,
        "rapid_num": None
    }, "000063.SZ", datetime.date(2017, 6, 7), datetime.date(2017, 6, 20))
    df.head()
