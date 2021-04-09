pragma foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    username text primary key,
    private_id text
);

CREATE TABLE IF NOT EXISTS posts (
    post_id text primary key,
    title text,
    content text,
    username text,
    timestamp int,
    upvotes int,
    FOREIGN KEY(username) REFERENCES users(username)
);