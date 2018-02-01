"""
    This file define an abstract interface of table structure definition classes.
"""


class TableStructure(object):
    """
        This class define some base functions used to define a table structure
        and generate create table sql.

        You need to implement "define_table_structure" function if you want to
        define a new table structure
    """
    def __init__(self):
        self.__columns = []
        self.__inited = False
        self.__schema = None
        self.__table = None

    def _is_inited(self):
        return self.__inited

    def _add_column(self, column):
        self.__columns.append(column)

    def _make_columns_str(self):
        return ",\n".join(["{0} {1}".format(column.name, column.type) for column in self.__columns])

    def define_table_structure(self):
        pass

    def get_table_define_sql(self, schema, table):
        if not self._is_inited():
            self.define_table_structure()
            self.__inited = True

        columns_str = self._make_columns_str()

        sql = """
            CREATE TABLE IF NOT EXISTS "{0}"."{1}"(
            {2}
            )
        """.format(schema, table, columns_str)

        return sql

    def get_column_name_list(self, without_quote=False):
        if not self._is_inited():
            self.define_table_structure()
            self.__inited = True

        ret_list = [column.name for column in self.__columns]

        if without_quote:
            ret_list = [name.replace("\"", "") for name in ret_list]

        return ret_list


class Column(object):
    def __init__(self, name, col_type):
        self.name = name
        self.type = col_type
