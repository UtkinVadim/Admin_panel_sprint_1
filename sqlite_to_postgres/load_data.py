import io
import uuid
import sqlite3
import datetime
import psycopg2

from dataclasses import dataclass
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as _connection


@dataclass(frozen=True)
class FilmWork:
    id: uuid
    title: str
    description: str
    creation_date: datetime
    certificate: str
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime

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


@dataclass(frozen=True)
class Genre:
    id: uuid
    name: str
    description: str
    created_at: datetime
    updated_at: datetime

    def fields(self):
        fields = (
            str(self.id),
            str(self.name),
            str(self.description) if self.description else 'null',
            str(self.created_at) if self.created_at else 'null',
            str(self.updated_at) if self.updated_at else 'null'
        )
        return fields


@dataclass(frozen=True)
class Person:
    id: uuid
    full_name: str
    birth_date: datetime
    created_at: datetime
    updated_at: datetime

    def fields(self):
        fields = (
            str(self.id),
            str(self.full_name),
            str(self.birth_date) if self.birth_date else 'null',
            str(self.created_at) if self.created_at else 'null',
            str(self.updated_at) if self.updated_at else 'null'
        )
        return fields


@dataclass(frozen=True)
class GenreFilmWork:
    id: uuid
    film_work_id: uuid
    genre_id: uuid
    created_at: datetime

    def fields(self):
        fields = (
            str(self.id),
            str(self.film_work_id),
            str(self.genre_id),
            str(self.created_at) if self.created_at else 'null'
        )
        return fields


@dataclass(frozen=True)
class PersonFilmWork:
    id: uuid
    film_work_id: uuid
    person_id: uuid
    role: str
    created_at: datetime

    def fields(self):
        fields = (
            str(self.id),
            str(self.film_work_id),
            str(self.person_id),
            str(self.role),
            str(self.created_at) if self.created_at else 'null'
        )
        return fields


class PostgresSaver:
    def __init__(self, pg_conn: _connection, data: dict):
        self.connect: _connection = pg_conn
        self.data = data
        self.delimiter = '|'

    def save_all_data(self):
        print("\nStart import!")
        print("-------------------------")
        start_import_time = datetime.datetime.now()
        with self.connect.cursor() as cursor:
            cursor.execute(open('../schema_design/db_schema.sql', 'r').read())
            self._save_film_work(cursor)
            self._save_genre(cursor)
            self._save_person(cursor)
            self._save_genre_film_work(cursor)
            self._save_person_film_work(cursor)
        print("-------------------------")
        print(f"All tables imported successfully. During time: {datetime.datetime.now() - start_import_time}")

    def _save_film_work(self, cursor: DictCursor) -> None:
        table_name = 'film_work'
        cursor.execute('TRUNCATE film_work')
        table_data = self._get_table_data(table_name=table_name)
        for film_work in table_data:
            data = io.StringIO()
            film_work_data = self.delimiter.join(film_work.fields())
            data.write(film_work_data)
            data.seek(0)
            cursor.copy_from(file=data,
                             table=table_name,
                             sep=self.delimiter,
                             null='null',
                             columns=['id', 'title', 'description', 'creation_date',
                                      'certificate', 'file_path', 'rating', 'type',
                                      'created_at', 'updated_at'])

            cursor.execute("""SELECT title FROM content.film_work WHERE id = '%s'""" % film_work.id)
            result = cursor.fetchone()
            assert str(result)[2:-2] == film_work.title

        cursor.execute("""SELECT count(*) FROM content.film_work""")
        result = cursor.fetchone()
        assert str(result).strip('[]') == str(len(table_data))
        print("Film work saving success!\n")

    def _save_genre(self, cursor: DictCursor) -> None:
        table_name = 'genre'
        cursor.execute("""TRUNCATE genre""")
        table_data = self._get_table_data(table_name=table_name)
        for genre in table_data:
            data = io.StringIO()
            genre_data = self.delimiter.join(genre.fields())
            data.write(genre_data)
            data.seek(0)
            cursor.copy_from(file=data,
                             table=table_name,
                             sep=self.delimiter,
                             null='null',
                             columns=['id', 'name', 'description', 'created_at', 'updated_at'])

            cursor.execute("""SELECT name FROM content.genre WHERE id = '%s'""" % genre.id)
            result = cursor.fetchone()
            assert str(result)[2:-2] == genre.name

        cursor.execute("""SELECT count(*) FROM content.genre""")
        result = cursor.fetchone()
        assert str(result).strip('[]') == str(len(table_data))
        print("Genre saving success!\n")

    def _save_person(self, cursor: DictCursor) -> None:
        table_name = 'person'
        cursor.execute("""TRUNCATE person""")
        table_data = self._get_table_data(table_name=table_name)
        for person in table_data:
            data = io.StringIO()
            person_data = self.delimiter.join(person.fields())
            data.write(person_data)
            data.seek(0)
            cursor.copy_from(file=data,
                             table=table_name,
                             sep=self.delimiter,
                             null='null',
                             columns=['id', 'full_name', 'birth_date', 'created_at', 'updated_at'])

            cursor.execute("""SELECT full_name FROM content.person WHERE id = '%s'""" % person.id)
            result = cursor.fetchone()
            assert str(result)[2:-2] == person.full_name

        cursor.execute("""SELECT count(*) FROM content.person""")
        result = cursor.fetchone()
        assert str(result).strip('[]') == str(len(table_data))
        print("Person saving success!\n")

    def _save_genre_film_work(self, cursor: DictCursor) -> None:
        table_name = 'genre_film_work'
        cursor.execute("""TRUNCATE genre_film_work""")
        table_data = self._get_table_data(table_name=table_name)
        for genre_film_work in table_data:
            data = io.StringIO()
            genre_film_work_data = self.delimiter.join(genre_film_work.fields())
            data.write(genre_film_work_data)
            data.seek(0)
            cursor.copy_from(file=data,
                             table=table_name,
                             sep=self.delimiter,
                             null='null',
                             columns=['id', 'film_work_id', 'genre_id', 'created_at'])

            cursor.execute("""SELECT film_work_id FROM content.genre_film_work WHERE id = '%s'""" % genre_film_work.id)
            result = cursor.fetchone()
            assert str(result)[2:-2] == genre_film_work.film_work_id

        cursor.execute("""SELECT count(*) FROM content.genre_film_work""")
        result = cursor.fetchone()
        assert str(result).strip('[]') == str(len(table_data))
        print("Genre - Film Work saving success!\n")

    def _save_person_film_work(self, cursor: DictCursor) -> None:
        table_name = 'person_film_work'
        cursor.execute("""TRUNCATE person_film_work""")
        table_data = self._get_table_data(table_name=table_name)
        for person_film_work in table_data:
            data = io.StringIO()
            person_film_work_data = self.delimiter.join(person_film_work.fields())
            data.write(person_film_work_data)
            data.seek(0)
            cursor.copy_from(file=data,
                             table=table_name,
                             sep=self.delimiter,
                             null='null',
                             columns=['id', 'film_work_id', 'person_id', 'role', 'created_at'])

            cursor.execute("""SELECT person_id FROM content.person_film_work WHERE id = '%s'""" % person_film_work.id)
            result = cursor.fetchone()
            assert str(result)[2:-2] == person_film_work.person_id

        cursor.execute("""SELECT count(*) FROM content.person_film_work""")
        result = cursor.fetchone()
        assert str(result).strip('[]') == str(len(table_data))
        print("Person - Film Work saving success!\n")

    def _get_table_data(self, table_name: str) -> list:
        return self.data[table_name]


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    @property
    def cur(self):
        return self.connection.cursor()

    def load_movies(self) -> dict:
        film_works = self._get_film_works_data()
        genres = self._get_genre_data()
        persons = self._get_persons_data()
        genre_film_works = self._get_genre_film_works_data()
        person_film_works = self._get_person_film_works_data()

        data = {
            "film_work": film_works,
            "genre": genres,
            "person": persons,
            "genre_film_work": genre_film_works,
            "person_film_work": person_film_works
        }
        self.cur.close()
        return data

    def _get_film_works_data(self) -> list:
        film_works = []
        for row in self.cur.execute(f'SELECT * FROM film_work'):
            film_works.append(FilmWork(*row))
        return film_works

    def _get_genre_data(self) -> list:
        genres = []
        for row in self.cur.execute(f'SELECT * FROM genre'):
            genres.append(Genre(*row))
        return genres

    def _get_persons_data(self):
        persons = []
        for row in self.cur.execute(f'SELECT * FROM person'):
            persons.append(Person(*row))
        return persons

    def _get_genre_film_works_data(self):
        genre_film_works = []
        for row in self.cur.execute(f'SELECT * FROM genre_film_work'):
            genre_film_works.append(GenreFilmWork(*row))
        return genre_film_works

    def _get_person_film_works_data(self):
        person_film_works = []
        for row in self.cur.execute(f'SELECT * FROM person_film_work'):
            person_film_works.append(PersonFilmWork(*row))
        return person_film_works


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
