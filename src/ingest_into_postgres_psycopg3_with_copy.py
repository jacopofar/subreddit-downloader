from datetime import datetime
import struct

import psycopg3
from loguru import logger

from src.ingest_helper import Submission, Comment, insertion_chunks, CONNECTION_STRING


def get_connection():
    return psycopg3.connect(CONNECTION_STRING)


PSQL_EPOCH = 946684800


def timestamp_to_binary(dt: int):
    return struct.pack(">q", int((dt - PSQL_EPOCH) * 10 ** 6))


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS submission (
            id           TEXT PRIMARY KEY,
            subreddit    TEXT,
            author       TEXT,
            created_utc  TIMESTAMP WITH TIME ZONE,
            title        TEXT,
            retrieved_at TIMESTAMP WITH TIME ZONE,
            score        SMALLINT,
            permalink    TEXT,
            LOCKED       BOOLEAN,
            selftext     TEXT,
            link         TEXT
        );
        CREATE TABLE IF NOT EXISTS comment (
            id           TEXT PRIMARY KEY,
            subreddit    TEXT,
            author       TEXT,
            body         TEXT,
            created_utc  TIMESTAMP WITH TIME ZONE,
            parent_id    TEXT,
            permalink    TEXT,
            score        SMALLINT,
            retrieved_at TIMESTAMP WITH TIME ZONE
        );
     """
        )


def upsert_submissions(conn, submissions: dict[str, Submission]):
    with conn.cursor(binary=True) as cur:
        cur.execute(
            """
        CREATE UNLOGGED TABLE new_submission AS SELECT * FROM submission WHERE FALSE;
        """
        )
        with cur.copy("COPY new_submission FROM STDIN WITH BINARY") as copy:
            for s in submissions.values():
                copy.write_row(
                    (
                        s.id,
                        s.subreddit,
                        s.author,
                        # datetime.fromtimestamp(s.created_utc),
                        timestamp_to_binary(s.created_utc),
                        s.title,
                        # datetime.fromtimestamp(s.retrieved_at),
                        timestamp_to_binary(s.retrieved_at),
                        s.score,
                        s.permalink,
                        s.locked,
                        s.selftext,
                        s.link,
                    )
                )
    stm = """
        INSERT INTO submission AS old (
                    id,
                    subreddit,
                    author,
                    created_utc,
                    title,
                    retrieved_at,
                    score,
                    permalink,
                    locked,
                    selftext,
                    link)
        SELECT * FROM new_submission
        ON CONFLICT(id) DO UPDATE SET
                author       = EXCLUDED.author,
                subreddit    = EXCLUDED.subreddit,
                created_utc  = EXCLUDED.created_utc,
                title        = EXCLUDED.title,
                retrieved_at = EXCLUDED.retrieved_at,
                score        = EXCLUDED.score,
                permalink    = EXCLUDED.permalink,
                locked       = EXCLUDED.locked,
                selftext     = EXCLUDED.selftext,
                link         = EXCLUDED.link
        WHERE
            EXCLUDED.retrieved_at >= old.retrieved_at;
     """

    with conn.cursor() as cur:
        cur.execute(stm)
        cur.execute("DROP TABLE new_submission;")

    conn.commit()


def upsert_comments(conn, comments: dict[str, Comment]):
    with conn.cursor(binary=True) as cur:
        cur.execute(
            """
        CREATE UNLOGGED TABLE new_comment AS SELECT * FROM comment WHERE FALSE;
        """
        )
        with cur.copy("COPY new_comment FROM STDIN WITH BINARY") as copy:
            for c in comments.values():
                copy.write_row(
                    (
                        c.id,
                        c.subreddit,
                        c.author,
                        c.body,
                        # datetime.fromtimestamp(c.created_utc),
                        timestamp_to_binary(c.created_utc),
                        c.parent_id,
                        c.permalink,
                        c.score,
                        # datetime.fromtimestamp(c.retrieved_at),
                        timestamp_to_binary(c.retrieved_at),
                    )
                )
    stm = """
         INSERT INTO comment AS old (
            id,
            subreddit,
            author,
            body,
            created_utc,
            parent_id,
            permalink,
            score,
            retrieved_at
         ) SELECT * FROM new_comment
        ON CONFLICT(id) DO UPDATE SET
            author = EXCLUDED.author,
            subreddit = EXCLUDED.subreddit,
            body = EXCLUDED.body,
            created_utc = EXCLUDED.created_utc,
            parent_id = EXCLUDED.parent_id,
            permalink = EXCLUDED.permalink,
            score = EXCLUDED.score,
            retrieved_at = EXCLUDED.retrieved_at
        WHERE EXCLUDED.retrieved_at >= old.retrieved_at;
     """

    with conn.cursor() as cur:
        cur.execute(stm)
        cur.execute("DROP TABLE new_comment;")
    conn.commit()


if __name__ == "__main__":
    conn = get_connection()
    create_tables(conn)
    total_subs, total_coms = 0, 0
    for subs, coms in insertion_chunks():
        total_subs += len(subs)
        total_coms += len(coms)
        upsert_submissions(conn, subs)
        logger.info(f"Submissions ingested so far: {total_subs}")
        upsert_comments(conn, coms)
        logger.info(f"Comments ingested so far: {total_coms}")
