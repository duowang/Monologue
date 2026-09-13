-- Postgres schema for `monologue import-db`.
CREATE TABLE IF NOT EXISTS monologue (
    id      serial PRIMARY KEY,
    author  varchar(80) NOT NULL,
    date    date        NOT NULL,
    source  varchar(20) NOT NULL,
    content text        NOT NULL,
    CONSTRAINT monologue_content_unique UNIQUE (content)
);

CREATE INDEX IF NOT EXISTS monologue_source_date_idx ON monologue (source, date);
CREATE INDEX IF NOT EXISTS monologue_author_idx ON monologue (author);
