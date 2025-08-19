import sys

from typing import List, Dict
from sqlalchemy import create_engine, select, delete, Column, Integer, String, MetaData, Table, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, IntegrityError, SQLAlchemyError

class DBInterface:
    """
        A wrapper for sqlalchemy database connections.
    Attributes: 
        engine: The engine from sqlalchemy
    """
    def __init__(self, connection_string: str, connect_args: {} = {}):
        """
            Intialize a DBInterface object
        Args:
            connection_string: the sqlalchemy connection string
        """
        self.engine = create_engine(connection_string, connect_args=connect_args, pool_recycle=3600)

        if self.engine is not None:
            self.connection = self.engine.connect()

        if 'sqlite' in connection_string:
            raw_conn = self.connection.connection
            cursor = raw_conn.cursor()

            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")
            cursor.execute("PRAGMA foreign_keys=ON;")

            cursor.close()

    def get_engine(self):
        """
            Get the db engine
        Returns:
            A reference to the db engine from sqlalchemy
        """
        return self.engine

    def query(self, sql: str):
        """
            Query using plain SQL
        Args:
            sql: The sql, as a string

        Returns:
            A result set.
        """
        result = self.connection.execute(text(sql))

        if result and hasattr(result, "fetchall"):
            return result.fetchall()

    def prepare_insertion(self, table: str, data: dict):
        """
            Helper to prepare an insertion 
        Args:
            table: The table to insert to
            data: The data to insert

        Returns:
            A prepared statement
        """
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = t.insert().values(
             **{key: value for key, value in data.items() if key in t.c}
        )

        return ps

    def insert_row(self, table: str, data: dict) -> int:
        """
            Insert one row to a table
        Args:
            table: The table name, as a string
            data: The data to insert, as key-value pairs in a dict

        Returns:
            An int, the last row's id
        """
        ps = self.prepare_insertion(table, data)

        result = self.connection.execute(ps)
        self.connection.commit()

        return result.lastrowid

    def update_row(self, table, data, atomic=False, **kwargs):
        """
            Update a row for a given table
        Args:
            table (): The table to insert to, as a string
            data (): The data to update, as key-value pairs in a dict
            atomic (): If True, will update the row as an atomic transaction
            **kwargs: 
        """
        t = Table(table, MetaData(), autoload_with=self.engine)

        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        ps = t.update().values(data).where(and_(*conditions))

        if atomic:
            try:
                self.connection.execute(text("BEGIN EXCLUSIVE"))
                self.connection.execute(ps)
                self.connection.commit()
            except Exception as e:
                print("Caught", e, file=sys.stderr)
                self.connection.rollback()
        else:
            self.connection.execute(ps)
            self.connection.commit()

    def aselect(self, table, col_name: str, fetch_one=True, **kwargs):
        """
            Select with AND for matching arguments
        Args:
            table (): The table to select from
            fetch_one (): If True, will just return the first result in the set
            col_name: The column name to select
            **kwargs: Column names corresponding to values used to perform the query

        Returns:
            A result set.
        """
        t = Table(table, MetaData(), autoload_with=self.engine)
        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        col = getattr(t.c, col_name)
        ps = select(col).where(and_(*conditions))

        compiled = ps.compile(self.engine, compile_kwargs={"literal_binds": True})
        res = self.connection.execute(compiled)

        if fetch_one:
            r = res.fetchone()
            return tuple(r) if r else None
        else:
            return res.fetchall()

    def aselectn(self, table, col_names: List[str], fetch_one=True, *args, **kwargs):
        """
            Select with AND for matching arguments
        Args:
            table (): The table to select from
            fetch_one (): If True, will just return the first result in the set
            col_name: The column name to select
            **kwargs: Column names corresponding to values used to perform the query

        Returns:
            A result set.
        """
        t = Table(table, MetaData(), autoload_with=self.engine)
        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        cols = [getattr(t.c, col_name) for col_name in col_names]
        ps = select(*cols).where(and_(*conditions))

        compiled = ps.compile(self.engine, compile_kwargs={"literal_binds": True})
        res = self.connection.execute(compiled)

        if fetch_one:
            return tuple(res.fetchone())
        else:
            return res.fetchall()

    def select_all(self, table):
        """
            Select all rows from a table
        Args:
            table (): The table to select from

        Returns:
            A result set.
        """
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = select(t).where()

        res = self.connection.execute(ps)
        return res.fetchall()

    def remove(self, table, column_name, column_value):
        """
            Remove from a table where one column_name matches
        Args:
            table (): The table to remove from
            column_name (): The column_name to match
            column_value (): The column_value to match
        """
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = delete(t).where(getattr(t.c, column_name) == column_value)

        self.connection.execute(ps)
        self.connection.commit()

    def removen(self, table, **kwargs):
        """
            Remove from a table where multiple columns match
        Args:
            table (): The table to remove from
            **kwargs: The columns to match
        """
        t = Table(table, MetaData(), autoload_with=self.engine)

        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        ps = delete(t).where(and_(*conditions))

        self.connection.execute(ps)
        self.connection.commit()

    def close(self):
        """
            Close the connection.
        """
        self.connection.close()
