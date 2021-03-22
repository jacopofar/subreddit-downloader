import asyncio
from datetime import datetime

import asyncpg
from loguru import logger

from src.ingest_helper import Submission, Comment, insertion_chunks, CONNECTION_STRING

# currently it takes 36 minutes to ingest 4.9M comments


async def get_connection():
    return await asyncpg.connect(dsn=CONNECTION_STRING)


async def create_tables(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS submission (
            id           TEXT PRIMARY KEY,
            subreddit    TEXT,
            author       TEXT,
            created_utc  TIMESTAMP WITH TIME ZONE,
            title        TEXT,
            retrieved_at TIMESTAMP WITH TIME ZONE,
            score        INTEGER,
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
            score        INTEGER,
            retrieved_at TIMESTAMP WITH TIME ZONE
        );
     """
    )


async def upsert_submissions(conn, submissions: dict[str, Submission]):
    stm = await conn.prepare(
        """
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
         ) VALUES ($1, $2, $3,$4, $5, $6, $7, $8, $9, $10, $11)
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
    )
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
    await stm.executemany(subs)


async def upsert_comments(conn, comments: dict[str, Comment]):
    stm = await conn.prepare(
        """
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
         ) VALUES ($1, $2, $3,$4, $5, $6, $7, $8, $9)
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
    )
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
    await stm.executemany(coms)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    conn = loop.run_until_complete(get_connection())
    loop.run_until_complete(create_tables(conn))
    total_subs, total_coms = 0, 0
    for subs, coms in insertion_chunks():
        total_subs += len(subs)
        total_coms += len(coms)
        loop.run_until_complete(upsert_submissions(conn, subs))
        logger.info(f"Submissions ingested so far: {total_subs}")
        loop.run_until_complete(upsert_comments(conn, coms))
        logger.info(f"Comments ingested so far: {total_coms}")
