# subreddit-text-downloader

Download all the text comments from a subreddit, and load them into a Postgres instance.

The Reddit downloader is based on the [original from pistocop](https://github.com/pistocop/reddit-downloader), refer to the original page for further details. This repository uses that data to demonstrate different methods to insert data in postgres.

This version stores some specific fields to take less space and allow an easier merge with existing data.

## Usage

Basic usage to download submissions and relative comments from
subreddit [AskReddit](https://www.reddit.com/r/AskReddit/) and [News](https://www.reddit.com/r/news/):

```shell
# Create a virtualkenv
python3 -m venv venv

# Install the dependencies
venv/bin/python3 -m pip install -r requirements.txt

# Download the AskReddit comments of the last 30 submissions
venv/bin/python3 src/subreddit_downloader.py AskReddit --batch-size 10 --laps 3 --reddit-id <reddit_id> --reddit-secret <reddit_secret> --reddit-username <reddit_username>

# Download the News comments after 1 January 2021
venv/bin/python3 src/subreddit_downloader.py AskReddit --batch-size 512 --laps 3 --reddit-id <reddit_id> --reddit-secret <reddit_secret> --reddit-username <reddit_username> --utc-after 1609459200

```

### Where I can get the reddit parameters?

- Parameters indicated with `<...>` on the previous script
- Official [Reddit guide](https://github.com/reddit-archive/reddit/wiki/OAuth2)
- TLDR: read this [stack overflow](https://stackoverflow.com/a/42304034)

| Parameter name | Description | How get it| Example of the value |
| --- | --- | --- | --- |
| `reddit_id` | The Client ID generated from the apps page | [Official guide](https://github.com/reddit-archive/reddit/wiki/OAuth2#authorization-implicit-grant-flow) | 40oK80pF8ac3Cn |
| `reddit_secret` | The secret generated from the apps page | Copy the value as showed [here](https://github.com/reddit-archive/reddit/wiki/OAuth2#getting-started) | 9KEUOE7pi8dsjs9507asdeurowGCcg|
| `reddit_username` | The reddit account name| The name you use for log in | pistoSniffer |



```bash
python src/subreddit_downloader.py --help
Usage: subreddit_downloader.py [OPTIONS] SUBREDDIT

  Download all the submissions and relative comments from a subreddit.

Arguments:
  SUBREDDIT  The subreddit name  [required]

Options:
  --output-dir TEXT               Optional output directory  [default:
                                  ./data/]

  --batch-size INTEGER            Request `batch_size` submission per time
                                  [default: 10]

  --laps INTEGER                  How many times request `batch_size` reddit
                                  submissions  [default: 3]

  --reddit-id TEXT                Reddit client_id, visit
                                  https://github.com/reddit-
                                  archive/reddit/wiki/OAuth2  [required]

  --reddit-secret TEXT            Reddit client_secret, visit
                                  https://github.com/reddit-
                                  archive/reddit/wiki/OAuth2  [required]

  --reddit-username TEXT          Reddit username, used for build the
                                  `user_agent` string, visit
                                  https://github.com/reddit-
                                  archive/reddit/wiki/API  [required]

  --utc-after TEXT                Fetch the submissions after this UTC date
  --utc-before TEXT               Fetch the submissions before this UTC date
  --debug / --no-debug            Enable debug logging  [default: False]
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.

  --help                          Show this message and exit.
```

## Ingest

This repository includes different scripts to ingest all the data in a Postgres instance, allowing for an incremental update of an existing database.

The Makefile contains a script to create and destroy a suitable dockerized postgres instance.

No parameters are required, for example:

    venv/bin/python3 src/ingest_into_postgres_psycopg3_with_copy.py

ingests the data into posgres, taking care of creating the tables and populating them, integrating with existing data and handling duplicates in the input.

The different scripts do the same thing using different techniques.
