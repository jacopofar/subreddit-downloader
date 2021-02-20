from dataclasses import dataclass
import json
import os
from pathlib import Path
import statistics
from time import time

from loguru import logger

# usually loaded form environment variables
# for the sake of ease of use here is hardcoded
CONNECTION_STRING = "postgres://postgres:mysecretpassword@127.0.0.1:5442/reddit"


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


def insertion_chunks(chunk_size: int = 50000):
    submissions: dict[str, Submission] = {}
    comments: dict[str, Comment] = {}
    insertion_times = []
    for root, _dirs, files in os.walk("data"):
        logger.debug(f"Processing folder {root}")
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
                logger.debug("pending size reached, will store in the DB...")
                start_time = time()
                yield submissions, comments
                spent = time() - start_time
                insertion_times.append(spent)
                logger.info(f"DB write took {spent:.0f} seconds")
                submissions = {}
                comments = {}
    logger.info(f"Average chunk insertion time: {statistics.mean(insertion_times):.1f}")
    logger.info(
        f"Median chunk insertion time: {statistics.median(insertion_times):.1f}"
    )
    logger.info(f"Standard deviation: {statistics.stdev(insertion_times):.1f}")
    # the remaining elements
    yield submissions, comments
