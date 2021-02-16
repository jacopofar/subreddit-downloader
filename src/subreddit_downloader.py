import sys
import json
from os.path import join
from pathlib import Path
from datetime import datetime
from time import time
from typing import Optional, Tuple

import typer
from typer import Argument
from typer import Option
from loguru import logger
from codetiming import Timer
from pushshift_py import PushshiftAPI
import praw
from prawcore.exceptions import NotFound


class HelpMessages:
    help_reddit_url = "https://github.com/reddit-archive/reddit/wiki/OAuth2"
    help_reddit_agent_url = "https://github.com/reddit-archive/reddit/wiki/API"

    subreddit = "The subreddit name"
    output_dir = "Optional output directory"
    batch_size = "Request `batch_size` submission per time"
    laps = "How many times request `batch_size` reddit submissions"
    reddit_id = f"Reddit client_id, visit {help_reddit_url}"
    reddit_secret = f"Reddit client_secret, visit {help_reddit_url}"
    reddit_username = f"Reddit username, used for build the `user_agent` string, visit {help_reddit_agent_url}"
    utc_after = "Fetch the submissions after this UTC date"
    utc_before = "Fetch the submissions before this UTC date"
    debug = "Enable debug logging"


class OutputManager:
    """
    Class used to collect and store data (submissions and comments)
    """

    params_filename = "params.json"

    def __init__(self, output_dir: str, subreddit: str):
        self.submissions_list = []
        self.comments_list = []
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.subreddit_dir = join(output_dir, subreddit)
        self.runtime_dir = join(self.subreddit_dir, self.run_id)

        self.submissions_output = join(self.runtime_dir, "submissions")
        self.comments_output = join(self.runtime_dir, "comments")
        self.params_path = join(self.runtime_dir, OutputManager.params_filename)

        self.total_submissions_counter = 0
        self.total_comments_counter = 0

        for path in [
            self.submissions_output,
            self.comments_output,
        ]:
            Path(path).mkdir(parents=True, exist_ok=True)

    def reset_lists(self):
        self.submissions_list = []
        self.comments_list = []

    def store(self, lap: int):
        now_ts = int(time())
        # Track total data statistics
        self.total_submissions_counter += len(self.submissions_list)
        self.total_comments_counter += len(self.comments_list)
        with open(join(self.submissions_output, f"{lap}.jsonl"), "a") as f:
            for sub in self.submissions_list:
                try:
                    sd = dict(
                        author=sub["author"],
                        id=sub["id"],
                        created_utc=sub["created_utc"],
                        title=sub["title"],
                        permalink=sub["permalink"],
                        score=sub["score"],
                        retrieved_at=now_ts,
                        locked=sub.get("locked", False),
                    )
                    if sub["is_self"]:
                        # sometimes is banned but locked=False :/
                        # https://www.reddit.com/r/redditdev/comments/7hfnew/there_is_currently_no_efficient_way_to_tell_if_a/
                        sd["selftext"] = sub.get("selftext", "")
                    else:
                        sd["link"] = sub["url"]
                except KeyError:
                    logger.warning(f"Offending submission entry: {sub}")
                    raise
                f.write(json.dumps(sd))
                f.write("\n")
        with open(join(self.comments_output, f"{lap}.jsonl"), "a") as f:
            for c in self.comments_list:
                try:
                    cd = dict(
                        id=c.id,
                        body=c.body,
                        created_utc=int(c.created_utc),
                        parent_id=c.parent_id,
                        permalink=c.permalink,
                        score=c.score,
                        retrieved_at=now_ts,
                    )
                    if c.author is not None:
                        cd["author"] = c.author.name
                    else:
                        cd["author"] = "[deleted]"
                except AttributeError:
                    logger.warning(f"Offending comment entry: {str(c)}")
                    raise
                f.write(json.dumps(cd))
                f.write("\n")

    def store_params(self, params: dict):
        with open(self.params_path, "w") as f:
            json.dump(params, f, indent=2)

    def load_params(self) -> dict:
        with open(self.params_path, "r") as f:
            params = json.load(f)
        return params

    def store_utc_params(self, utc_older: int, utc_newer: int):
        params = self.load_params()
        params["utc_older"] = utc_older
        params["utc_newer"] = utc_newer
        self.store_params(params)


def init_clients(
    reddit_id: str, reddit_secret: str, reddit_username: str
) -> Tuple[PushshiftAPI, praw.Reddit]:
    pushshift_api = PushshiftAPI()

    reddit_api = praw.Reddit(
        client_id=reddit_id,
        client_secret=reddit_secret,
        user_agent=f"python_script:subreddit_downloader:(by /u/{reddit_username})",
    )

    return pushshift_api, reddit_api


def init_locals(
    debug: bool,
    output_dir: str,
    subreddit: str,
    utc_after: Optional[int],
    utc_before: Optional[int],
    run_args: dict,
) -> Tuple[str, OutputManager]:
    assert not (
        utc_after and utc_before
    ), "`utc_before` and `utc_after` parameters are in mutual exclusion"
    run_args.pop("reddit_secret")

    if not debug:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    direction = "after" if utc_after else "before"
    output_manager = OutputManager(output_dir, subreddit)

    output_manager.store_params(run_args)
    return direction, output_manager


def comments_fetcher(sub, output_manager, reddit_api):
    """
    Comments fetcher
    Get all comments with depth-first approach
    Solution from https://praw.readthedocs.io/en/latest/tutorials/comments.html
    """
    try:
        submission_rich_data = reddit_api.submission(id=sub.id)
        submission_rich_data.comments.replace_more(limit=None)
        comments = submission_rich_data.comments.list()
    except NotFound:  # Submission found on pushshift but not in praw
        logger.warning(
            f"Submission not found in PRAW: `{sub.id}` - `{sub.title}` - `{sub.full_link}`"
        )
        return
    for comment in comments:
        output_manager.comments_list.append(comment)


def utc_range_calculator(
    utc_received: int, utc_after: Optional[int], utc_before: Optional[int]
) -> Tuple[int, int]:
    """Calculate the max UTC range seen.

    Increase/decrease utc_after/utc_before according with utc_received value
    """
    if utc_after is None or utc_before is None:
        utc_after = utc_received
        utc_before = utc_received

    utc_before = min(utc_before, utc_received)
    utc_after = max(utc_after, utc_received)

    return utc_after, utc_before


@Timer(name="main", text="Total downloading time: {minutes:.1f}m", logger=logger.info)
def main(
    subreddit: str = Argument(..., help=HelpMessages.subreddit),
    output_dir: str = Option("./data/", help=HelpMessages.output_dir),
    batch_size: int = Option(10, help=HelpMessages.batch_size),
    laps: int = Option(3, help=HelpMessages.laps),
    reddit_id: str = Option(..., help=HelpMessages.reddit_id),
    reddit_secret: str = Option(..., help=HelpMessages.reddit_secret),
    reddit_username: str = Option(..., help=HelpMessages.reddit_username),
    utc_after: Optional[int] = Option(None, help=HelpMessages.utc_after),
    utc_before: Optional[int] = Option(None, help=HelpMessages.utc_before),
    debug: bool = Option(False, help=HelpMessages.debug),
):
    """
    Download all the submissions and relative comments from a subreddit.
    """

    # Init
    direction, out_manager = init_locals(
        debug, output_dir, subreddit, utc_after, utc_before, run_args=locals()
    )
    pushshift_api, reddit_api = init_clients(reddit_id, reddit_secret, reddit_username)
    logger.info(
        f"Start download: "
        f"UTC range: [{utc_before}, {utc_after}], "
        f"direction: `{direction}`, "
        f"batch size: {batch_size}, "
        f"total submissions to fetch: {batch_size * laps}"
    )

    # Start the gathering
    for lap in range(laps):
        lap_message = f"Lap {lap}/{laps} completed in " "{minutes:.1f}m"
        with Timer(text=lap_message, logger=logger.info):

            # Reset the data already stored
            out_manager.reset_lists()

            # Fetch data in the `direction` way
            submissions_generator = pushshift_api.search_submissions(
                subreddit=subreddit,
                limit=batch_size,
                sort="desc",
                sort_type="created_utc",
                after=utc_after if direction == "after" else None,
                before=utc_before if direction == "before" else None,
            )

            for sub in submissions_generator:
                # Fetch the submission data
                out_manager.submissions_list.append(sub.d_)

                # Fetch the submission's comments
                comments_fetcher(sub, out_manager, reddit_api)

                # Calculate the UTC seen range
                utc_after, utc_before = utc_range_calculator(
                    sub.created_utc, utc_after, utc_before
                )

            # Store data (submission and comments)
            out_manager.store(lap)
            logger.info(f"Stored comments: {len(out_manager.comments_list)}")
        logger.info(
            f"utc_after: {utc_after} ({datetime.fromtimestamp(utc_after).isoformat()}), "
            f"utc_before: {utc_before} ({datetime.fromtimestamp(utc_before).isoformat()})"
        )
    out_manager.store_utc_params(utc_newer=utc_after, utc_older=utc_before)

    logger.info(
        f"Stop download: lap {laps}/{laps} [total]: {out_manager.total_comments_counter}"
    )


if __name__ == "__main__":
    typer.run(main)
