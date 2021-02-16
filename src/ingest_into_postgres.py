import asyncio
from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path

import asyncpg


@dataclass
class Submission:
    author: str
    id: str
    created_utc: int
    title: str
    retrieved_at: int
    score: int
    permalink: str
    locked: bool
    selftext: str
    link: str
    subreddit: str


@dataclass
class Comment:
    id: str
    author: str
    body: str
    created_utc: int
    parent_id: str
    permalink: str
    score: int
    retrieved_at: int
    subreddit: str


async def get_connection():
    return await asyncpg.connect(
        dsn="postgres://postgres:mysecretpassword@127.0.0.1:5442/reddit"
    )


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


async def upsert_comments(conn, comments):
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


def merge_submission(submissions: dict[str, Submission], obj, sub_name: str):
    if (
        obj["id"] in submissions
        and submissions[obj["id"]].retrieved_at > obj["retrieved_at"]
    ):
        return
    submissions[obj["id"]] = Submission(
        id=obj["id"],
        author=obj["author"],
        created_utc=obj["created_utc"],
        title=obj["title"],
        retrieved_at=obj["retrieved_at"],
        score=obj["score"],
        permalink=obj["permalink"],
        locked=obj["locked"],
        selftext=obj.get("selftext"),
        link=obj.get("link"),
        subreddit=sub_name,
    )


def merge_comment(comments: dict[str, Comment], obj, sub_name: str):
    if obj["id"] in comments and comments[obj["id"]].retrieved_at > obj["retrieved_at"]:
        return
    comments[obj["id"]] = Comment(
        id=obj["id"],
        author=obj["author"],
        created_utc=obj["created_utc"],
        retrieved_at=obj["retrieved_at"],
        score=obj["score"],
        permalink=obj["permalink"],
        body=obj["body"],
        parent_id=obj["parent_id"],
        subreddit=sub_name,
    )


def insertion_chunks(chunk_size: int = 10000):
    submissions = {}
    comments = {}

    for root, _dirs, files in os.walk("data"):
        print(f"Processing folder {root}")
        for fname in files:
            if not fname.endswith(".jsonl"):
                continue
            subname = root.split("/")[1]
            with open(Path(root) / fname) as fr:
                if root.endswith("submissions"):
                    for line in fr:
                        merge_submission(submissions, json.loads(line), subname)
                elif root.endswith("comments"):
                    for line in fr:
                        merge_comment(comments, json.loads(line), subname)
                else:
                    raise ValueError(f"Unknown file {root} -> {fname}")
            if len(submissions) + len(comments) > chunk_size:
                print("pending size reached, will store in the DB...")
                yield submissions, comments
                submissions = {}
                comments = {}
    # generate the remaining elements
    yield submissions, comments


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    conn = loop.run_until_complete(get_connection())
    loop.run_until_complete(create_tables(conn))

    for subs, coms in insertion_chunks():
        loop.run_until_complete(upsert_submissions(conn, subs))
        loop.run_until_complete(upsert_comments(conn, coms))
