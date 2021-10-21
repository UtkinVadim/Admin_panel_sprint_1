import datetime
import io
import os
import sqlite3
import uuid
from dataclasses import dataclass
from typing import List

import psycopg2
from dotenv import find_dotenv, load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

load_dotenv(find_dotenv(raise_error_if_not_found=False))

DELIMITER = "|"


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
class Filmwork(AbsDataclass):
    title: str
    description: str
    creation_date: datetime
    certificate: str
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime
    table_name: str = "filmwork"

    def fields(self) -> tuple:
        fields = (
            str(self.id),
            str(self.title),
            str(self.description) if self.description else "null",
            str(self.creation_date) if self.creation_date else "null",
            str(self.certificate) if self.certificate else "null",
            str(self.file_path) if self.file_path else "null",
            str(self.rating) if self.rating else "null",
            str(self.type),
            str(self.created_at) if self.created_at else "null",
            str(self.updated_at) if self.updated_at else "null",
        )
        return fields

    def field_names(self) -> list:
        return [
            "id",
            "title",
            "description",
            "creation_date",
            "certificate",
            "file_path",
            "rating",
            "type",
            "created_at",
            "updated_at",
        ]


@dataclass(frozen=True)
class Genre(AbsDataclass):
    name: str
    description: str
    created_at: datetime
    updated_at: datetime
    table_name: str = "genre"

    def fields(self) -> tuple:
        fields = (
            str(self.id),
            str(self.name),
            str(self.description) if self.description else "null",
            str(self.created_at) if self.created_at else "null",
            str(self.updated_at) if self.updated_at else "null",
        )
        return fields

    def field_names(self) -> list:
        return ["id", "name", "description", "created_at", "updated_at"]


@dataclass(frozen=True)
class Person(AbsDataclass):
    full_name: str
    birth_date: datetime
    created_at: datetime
    updated_at: datetime
    table_name: str = "person"

    def fields(self):
        fields = (
            str(self.id),
            str(self.full_name),
            str(self.birth_date) if self.birth_date else "null",
            str(self.created_at) if self.created_at else "null",
            str(self.updated_at) if self.updated_at else "null",
        )
        return fields

    def field_names(self) -> list:
        return ["id", "full_name", "birth_date", "created_at", "updated_at"]


@dataclass(frozen=True)
class GenreFilmwork(AbsDataclass):
    filmwork_id: uuid
    genre_id: uuid
    created_at: datetime
    table_name: str = "genre_filmwork"

    def fields(self):
        fields = (
            str(self.filmwork_id),
            str(self.genre_id),
            str(self.created_at) if self.created_at else "null",
        )
        return fields

    def field_names(self) -> list:
        return ["filmwork_id", "genre_id", "created_at"]


@dataclass(frozen=True)
class PersonFilmwork(AbsDataclass):
    filmwork_id: uuid
    person_id: uuid
    role: str
    created_at: datetime
    table_name: str = "person_filmwork"

    def fields(self):
        fields = (
            str(self.filmwork_id),
            str(self.person_id),
            str(self.role),
            str(self.created_at) if self.created_at else "null",
        )
        return fields

    def field_names(self) -> list:
        return ["filmwork_id", "person_id", "role", "created_at"]


class PostgresSaver:
    def __init__(self, pg_conn: _connection, table_data: list = None):
        self.connect: _connection = pg_conn
        self.cursor: DictCursor = self.connect.cursor()
        self.table_data: list = table_data
        self.tables_for_import = [
            "filmwork",
            "genre",
            "person",
            "genre_filmwork",
            "person_filmwork",
        ]

    @property
    def table_instance(self) -> dataclass:
        return self.table_data[0]

    def create_db_schema(self):
        with open("../schema_design/db_schema.sql", "r") as db_schema:
            self.cursor.execute(db_schema.read())

    def clear_tables_for_import(self):
        for table in self.tables_for_import:
            self.cursor.execute(f"TRUNCATE content.{table} CASCADE")

    def save_data(self) -> None:
        data_for_import = io.StringIO()
        for table in self.table_data:
            data_for_import.write(table.data_to_write())
        self._copy(data_for_import)

    def _copy(self, data_for_import: io.StringIO) -> io.StringIO:
        try:
            data_for_import.seek(0)
            self.cursor.copy_from(
                file=data_for_import,
                table=self.table_instance.table_name,
                sep=DELIMITER,
                null="null",
                columns=self.table_instance.field_names(),
            )
        except psycopg2.Error as err:
            raise ValueError(f"Writing error: {err.pgerror}")
        else:
            return io.StringIO()


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection: sqlite3.Connection = connection
        self.tables_for_export = [
            "film_work",
            "genre",
            "person",
            "genre_film_work",
            "person_film_work",
        ]

    @property
    def _table_dataclass_handler(self) -> dict:
        return {
            "film_work": Filmwork,
            "genre": Genre,
            "person": Person,
            "genre_film_work": GenreFilmwork,
            "person_film_work": PersonFilmwork,
        }

    def table_data_generator(self) -> List[dataclass]:
        for table_name in self.tables_for_export:
            table_dataclass = self._table_dataclass_handler[table_name]
            limit = 500
            offset = 0
            try:
                count_rows_for_export = self.connection.execute(
                    f"SELECT count(*) FROM {table_name}"
                ).fetchone()[0]
                while count_rows_for_export > 0:
                    count_rows_for_export -= limit
                    rows = self.connection.cursor().execute(
                        f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
                    )
                    offset += limit
                    yield [table_dataclass(*row) for row in rows]
            except sqlite3.OperationalError as err:
                raise ValueError(f"Read error: {err}")


def check_loaded_data(
    sqlite_connection: sqlite3.Connection,
    pg_connection: _connection,
    tables_for_checking: zip,
):
    pg_cursor: DictCursor = pg_connection.cursor()
    for sqlite_table, pg_table in tables_for_checking:
        rows_count_in_sqlite_db = sqlite_connection.execute(
            f"SELECT count(*) FROM {sqlite_table}"
        ).fetchone()[0]
        pg_cursor.execute(f"SELECT count(*) FROM content.{pg_table}")
        rows_count_in_pg_db = pg_cursor.fetchone()[0]
        assert rows_count_in_sqlite_db == rows_count_in_pg_db


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres."""
    sqlite_loader = SQLiteLoader(connection)
    data_for_import = sqlite_loader.table_data_generator()

    postgres_saver = PostgresSaver(pg_conn)
    postgres_saver.create_db_schema()
    postgres_saver.clear_tables_for_import()

    for table_data in data_for_import:
        postgres_saver.table_data = table_data
        postgres_saver.save_data()

    tables_for_checking = zip(
        sqlite_loader.tables_for_export, postgres_saver.tables_for_import
    )

    check_loaded_data(connection, pg_conn, tables_for_checking)

    print("Import completed!")


if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
        "options": "-c search_path=content",
    }
    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
