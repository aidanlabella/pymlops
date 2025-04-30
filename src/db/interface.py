from typing import List, Dict
from sqlalchemy import create_engine, select, delete, Column, Integer, String, MetaData, Table, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, IntegrityError, SQLAlchemyError

class DBInterface:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, pool_recycle=3600)

        if self.engine is not None:
            self.connection = self.engine.connect()

    def get_engine(self):
        return self.engine

    def query(self, sql: str):
        result = self.connection.execute(text(sql))

        if result and hasattr(result, "fetchall"):
            return result.fetchall()

    def prepare_insertion(self, table: str, data: dict):
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = t.insert().values(
             **{key: value for key, value in data.items() if key in t.c}
        )

        return ps

    def insert_row(self, table: str, data: dict) -> int:
        ps = self.prepare_insertion(table, data)

        result = self.connection.execute(ps)
        self.connection.commit()

        return result.lastrowid

    def update_row(self, table, data, atomic=False, **kwargs):
        t = Table(table, MetaData(), autoload_with=self.engine)

        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        ps = t.update().values(data).where(and_(*conditions))

        if atomic:
            trans = self.connection.begin()
            try:
                self.connection.execute(ps)
                trans.commit()
            except Exception as e:
                print("Caught", e, file=sys.stderr)
                trans.rollback()
        else:
            self.connection.execute(ps)
            self.connection.commit()

    def aselect(self, table, col_name: str, fetch_one=True, *args, **kwargs):
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
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = select(t).where()

        res = self.connection.execute(ps)
        return res.fetchall()

    def remove(self, table, column_name, column_value):
        t = Table(table, MetaData(), autoload_with=self.engine)
        ps = delete(t).where(getattr(t.c, column_name) == column_value)

        self.connection.execute(ps)
        self.connection.commit()

    def removen(self, table, *args, **kwargs):
        t = Table(table, MetaData(), autoload_with=self.engine)

        conditions = []
        for key, value in kwargs.items():
            conditions.append(getattr(t.c, key) == value)

        ps = delete(t).where(and_(*conditions))

        res = self.connection.execute(ps)
        self.connection.commit()

        return res

    def close(self):
        self.connection.close()
