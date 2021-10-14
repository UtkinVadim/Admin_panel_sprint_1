import io
import uuid
import sqlite3
import datetime
import psycopg2

from typing import List
from dataclasses import dataclass
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as _connection

DELIMITER = '|'


@dataclass(frozen=True)
class AbsDataclass:
    id: uuid

    def fields(self) -> tuple:
        raise NotImplementedError

    def field_names(self) -> list:
        raise NotImplementedError

    def data_to_write(self) -> str:
        return DELIMITER.join(self.fields()) + "\n"


@dataclass(frozen=True)
class FilmWork(AbsDataclass):
    title: str
    description: str
    creation_date: datetime
    certificate: str
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime
    table_name: str = 'film_work'

    def fields(self) -> tuple:
        fields = (
            str(self.id),
            str(self.title),
            str(self.description) if self.description else 'null',
            str(self.creation_date) if self.creation_date else 'null',
            str(self.certificate) if self.certificate else 'null',
            str(self.file_path) if self.file_path else 'null',
            str(self.rating) if self.rating else 'null',
            str(self.type),
            str(self.created_at) if self.created_at else 'null',
            str(self.updated_at) if self.updated_at else 'null'
        )
        return fields

    def field_names(self) -> list:
        return ['id',
                'title',
                'description',
                'creation_date',
                'certificate',
                'file_path',
                'rating',
                'type',
                'created_at',
                'updated_at']


@dataclass(frozen=True)
class Genre(AbsDataclass):
    name: str
    description: str
    created_at: datetime
    updated_at: datetime
    table_name: str = 'genre'

    def fields(self) -> tuple:
        fields = (
            str(self.id),
            str(self.name),
            str(self.description) if self.description else 'null',
            str(self.created_at) if self.created_at else 'null',
            str(self.updated_at) if self.updated_at else 'null'
        )
        return fields

    def field_names(self) -> list:
        return ['id', 'name', 'description', 'created_at', 'updated_at']


@dataclass(frozen=True)
class Person(AbsDataclass):
    full_name: str
    birth_date: datetime
    created_at: datetime
    updated_at: datetime
    table_name: str = 'person'

    def fields(self):
        fields = (
            str(self.id),
            str(self.full_name),
            str(self.birth_date) if self.birth_date else 'null',
            str(self.created_at) if self.created_at else 'null',
            str(self.updated_at) if self.updated_at else 'null'
        )
        return fields

    def field_names(self) -> list:
        return ['id', 'full_name', 'birth_date', 'created_at', 'updated_at']


@dataclass(frozen=True)
class GenreFilmWork(AbsDataclass):
    film_work_id: uuid
    genre_id: uuid
    created_at: datetime
    table_name: str = 'genre_film_work'

    def fields(self):
        fields = (
            str(self.id),
            str(self.film_work_id),
            str(self.genre_id),
            str(self.created_at) if self.created_at else 'null'
        )
        return fields

    def field_names(self) -> list:
        return ['id', 'film_work_id', 'genre_id', 'created_at']


@dataclass(frozen=True)
class PersonFilmWork(AbsDataclass):
    film_work_id: uuid
    person_id: uuid
    role: str
    created_at: datetime
    table_name: str = 'person_film_work'

    def fields(self):
        fields = (
            str(self.id),
            str(self.film_work_id),
            str(self.person_id),
            str(self.role),
            str(self.created_at) if self.created_at else 'null'
        )
        return fields

    def field_names(self) -> list:
        return ['id', 'film_work_id', 'person_id', 'role', 'created_at']


class PostgresSaver:
    def __init__(self, pg_conn: _connection, data: dict):
        self.connect: _connection = pg_conn
        self.data = data
        self.cursor: DictCursor = self.connect.cursor()
        self.insert_limit: int = 200

    def save_all_data(self) -> None:
        self.cursor.execute(open('../schema_design/db_schema.sql', 'r').read())
        for table_name, tables in self.data.items():
            self.cursor.execute(f'TRUNCATE {table_name}')
            self._write_data_from_tables(tables)
            self._check_load_data(tables)
        self.cursor.close()

    def _write_data_from_tables(self, tables: List[dataclass]) -> None:
        data = io.StringIO()
        table_instance = tables[0]
        data_count = 0
        for table in tables:
            data_count += 1
            data.write(table.data_to_write())
            if data_count == self.insert_limit:
                data = self._copy(data, table_instance)
                data_count = 0

        self._copy(data, table_instance)

    def _copy(self, data: io.StringIO, table: dataclass) -> io.StringIO:
        try:
            data.seek(0)
            self.cursor.copy_from(file=data,
                                  table=table.table_name,
                                  sep=DELIMITER,
                                  null='null',
                                  columns=table.field_names())
        except psycopg2.Error as err:
            raise ValueError(f'Writing error: {err.pgerror}')
        else:
            return io.StringIO()

    def _check_load_data(self, tables: list) -> None:
        table_instance = tables[0]
        self.cursor.execute(f'SELECT count(*) FROM content.{table_instance.table_name}')
        result = self.cursor.fetchone()
        assert str(result).strip('[]') == str(len(tables))
        print(f"{table_instance.table_name.replace('_', ' ').title()} saving success!\n")


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection: sqlite3.Connection = connection

    @property
    def cur(self) -> sqlite3.Cursor:
        return self.connection.cursor()

    @property
    def _table_dataclass_handler(self) -> dict:
        return {
            "film_work": FilmWork,
            "genre": Genre,
            "person": Person,
            "genre_film_work": GenreFilmWork,
            "person_film_work": PersonFilmWork
        }

    def load_movies(self) -> dict:
        try:
            data = {
                "film_work": self._get_data_from_table(table_name="film_work"),
                "genre": self._get_data_from_table(table_name="genre"),
                "person": self._get_data_from_table(table_name="person"),
                "genre_film_work": self._get_data_from_table(table_name="genre_film_work"),
                "person_film_work": self._get_data_from_table(table_name="person_film_work"),
            }
        except sqlite3.OperationalError as err:
            raise ValueError(f'Read error: {err}')
        else:
            return data
        finally:
            self.cur.close()

    def _get_data_from_table(self, table_name: str) -> dataclass:
        tables = []
        table_dataclass = self._table_dataclass_handler[table_name]
        for row in self.cur.execute(f'SELECT * FROM {table_name}'):
            tables.append(table_dataclass(*row))
        return tables


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_loader = SQLiteLoader(connection)
    data = sqlite_loader.load_movies()

    postgres_saver = PostgresSaver(pg_conn, data)
    postgres_saver.save_all_data()


if __name__ == '__main__':
    dsn = {
        'dbname': 'movies',
        'user': 'movies',
        'password': 'movies',
        'host': '127.0.0.1',
        'port': 5432,
        'options': '-c search_path=content'
    }
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsn, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
