from datetime import datetime

import psycopg3
from loguru import logger

from src.ingest_helper import Submission, Comment, insertion_chunks, CONNECTION_STRING


def get_connection():
    return psycopg3.connect(CONNECTION_STRING)


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
            link
         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            author = EXCLUDED.author,
            subreddit = EXCLUDED.subreddit,
            created_utc = EXCLUDED.created_utc,
            title = EXCLUDED.title,
            retrieved_at = EXCLUDED.retrieved_at,
            score = EXCLUDED.score,
            permalink = EXCLUDED.permalink,
            locked = EXCLUDED.locked,
            selftext = EXCLUDED.selftext,
            link = EXCLUDED.link
        WHERE EXCLUDED.retrieved_at >= old.retrieved_at;
     """
    subs = []
    for s in submissions.values():
        subs.append(
            [
                s.id,
                s.subreddit,
                s.author,
                datetime.fromtimestamp(s.created_utc),
                s.title,
                datetime.fromtimestamp(s.retrieved_at),
                s.score,
                s.permalink,
                s.locked,
                s.selftext,
                s.link,
            ]
        )
    with conn.cursor() as cur:
        # cur.executemany(stm, subs)
        for sub in subs:
            cur.execute(stm, sub, prepare=True)

    conn.commit()


def upsert_comments(conn, comments: dict[str, Comment]):
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
         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    coms = []
    for c in comments.values():
        coms.append(
            [
                c.id,
                c.subreddit,
                c.author,
                c.body,
                datetime.fromtimestamp(c.created_utc),
                c.parent_id,
                c.permalink,
                c.score,
                datetime.fromtimestamp(c.retrieved_at),
            ]
        )
    with conn.cursor() as cur:
        # cur.executemany(stm, coms)
        for com in coms:
            cur.execute(stm, com, prepare=True)
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
