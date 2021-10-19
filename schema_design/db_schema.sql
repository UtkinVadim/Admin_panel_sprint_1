-- Создание отдельной схемы для контента:
CREATE SCHEMA IF NOT EXISTS content;

-- Кинопроизведения:
CREATE TABLE IF NOT EXISTS content.filmwork (
    id uuid NOT NULL PRIMARY KEY,
    title varchar(255) NOT NULL,
    description text,
    creation_date date,
    certificate text,
    file_path varchar(100),
    rating double precision,
    type varchar(20) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


-- Жанры кинопроизведений:
CREATE TABLE IF NOT EXISTS content.genre (
    id uuid NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    description text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


-- Актеры:
CREATE TABLE IF NOT EXISTS content.person (
    id uuid NOT NULL PRIMARY KEY,
    full_name varchar(255) NOT NULL,
    birth_date date,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


-- Таблица, которая связывает кинопроизведение и жанр:
CREATE TABLE IF NOT EXISTS content.genre_filmwork (
    id bigserial NOT NULL PRIMARY KEY,
    filmwork_id uuid NOT NULL,
    genre_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT fk_filmwork_genre FOREIGN KEY (filmwork_id) REFERENCES content.filmwork (id) ON UPDATE CASCADE  ON DELETE CASCADE,
    CONSTRAINT fk_genre FOREIGN KEY (genre_id) REFERENCES content.genre (id) ON UPDATE CASCADE
);


-- Таблица, которая связывает кинопроизведение и актера:
CREATE TABLE IF NOT EXISTS content.person_filmwork (
    id bigserial NOT NULL PRIMARY KEY,
    filmwork_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role varchar(255) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT fk_filmwork_person FOREIGN KEY (filmwork_id) REFERENCES content.filmwork (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES content.person (id) ON UPDATE CASCADE
);

-- Уникальный композитный индекс для кинопроизведения и жанра:
CREATE UNIQUE INDEX IF NOT EXISTS filmwork_genre ON content.genre_filmwork (filmwork_id, genre_id);

-- Уникальный композитный индекс для кинопроизведения, актера и жанра:
CREATE UNIQUE INDEX IF NOT EXISTS person_filmwork_role ON content.person_filmwork (filmwork_id, person_id, role);