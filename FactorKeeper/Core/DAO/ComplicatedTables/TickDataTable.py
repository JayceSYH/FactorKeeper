from Core.DAO.ComplicatedTables.TableDefinition import TableStructure, Column


class TickDataTable(TableStructure):
    """
        This class defines an example tick data table structure adapted
        to stock tick data fetch from wind.

        This table contains following columns:
            ask(n) --current ask price, n belongs to 1-10
            bid(n) --current bid price, n belongs to 1-10
            asize(n) --current ask size, n belongs to 1-10
            bsize(n) --current bid size, n belongs to 1-10
            volume --last trade volume
            last --last trade price
            datetime --current timestamp
            date --current date

    """
    def define_table_structure(self):
        for i in range(1, 11):
            self._add_column(Column("ask" + str(i), "double precision"))
            self._add_column(Column("bid" + str(i), "double precision"))
            self._add_column(Column("asize" + str(i), "double precision"))
            self._add_column(Column("bsize" + str(i), "double precision"))

        self._add_column(Column("datetime", "timestamp without time zone"))
        self._add_column(Column("\"date\"", "date NOT NULL"))
        self._add_column(Column("id", "serial NOT NULL PRIMARY KEY"))
        self._add_column(Column("\"last\"", "double precision"))
        self._add_column(Column("volume", "double precision"))
