from Core.DAO.ComplicatedTables.TableDefinition import TableStructure, Column


class StockViewTable(TableStructure):
    """
        This class defines stock view data table structure
    """
    def __init__(self, stock_relation):
        super().__init__()
        self.__stock_relation = stock_relation

    def define_table_structure(self):
        for stock in self.__stock_relation:
            for column in self.__stock_relation[stock]:
                self._add_column(Column("\"{0}_{1}\"".format(column, stock), "double precision"))
        self._add_column(Column("datetime", "timestamp without time zone"))
        self._add_column(Column("\"date\"", "date NOT NULL"))
        self._add_column(Column("id", "serial NOT NULL PRIMARY KEY"))
