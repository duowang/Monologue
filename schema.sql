CREATE TABLE monologue_test (
    author      varchar(20),
    date        date,
    source      varchar(20),
    content     text,
    CONSTRAINT content UNIQUE(content)
);
