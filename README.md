# Slack Archiver

Create a local archive of your slack messages. Slack admin privileges not required.

## Install

```console
cargo install
```

## Usage

First, [Generate a Slack API token](https://api.slack.com/custom-integrations/legacy-tokens) for the workspace you want to archive.

Then, run the archive command save all messages since the last run. Archived messages are not duplicated.

```console
TOKEN=<slack api token> \
DB_PATH=</path/to/your.db> \
slack_archive
```

This command can be run periodically to archive your messages before slack eats them.
