use std::env;

use failure::{Error, ResultExt};
use slack;
use rusqlite;

/// Number of messages to return for each pagination query
const PAGE_SIZE: u32 = 1000; // max allowed by slack api

/// The expected time window between when a message is first written
/// and when it is last edited.
///
/// Since there isn't a direct way to search for messages that have been
/// edited, we search for all messages starting at EDIT_WINDOW_MINUTES
/// before the last archive time. The results can include duplicate
/// messages, some of which may be edits.
///
/// Edits made outside of this time window will not be captured.
///
/// Setting this to 0 ensures that no edits will be captured, and
/// zero duplicate messages will be fetched.
///
/// Setting this to a very high number ensures that all edits are
/// captured, but the entire message history is fetched on every
/// archive run (duplicate messages are deduped when stored).
const EDIT_WINDOW_MINUTES: i64 = 60;

pub fn archive() -> Result<(), Error> {
    let token = env::var("SLACK_API_TOKEN").expect("SLACK_API_TOKEN not set.");
    let data_dir = env::var("DATA_DIR").unwrap_or("./data".to_string());
    let client = slack::default_client().unwrap();

    let db_path = format!("{}/archive.db", data_dir);
    let db = init_db(&db_path)?;

    archive_all(&db, &client, &token)?;

    return Ok(());
}

pub fn archive_all(
    db: &rusqlite::Connection,
    client: &slack::requests::Client,
    token: &str,
) -> Result<(), Error> {
    let response = slack::channels::list(client, token, &slack::channels::ListRequest::default())?;

    if let Some(channels) = response.channels {
        for channel in channels {
            if let (Some(id), Some(name)) = (channel.id, channel.name) {
                if name != "general" {
                    continue;
                }
                println!("Archiving channel: {} ({})", name, id);
                archive_channel(db, client, token, &id)?;
            }
        }
    }
    db.execute("PRAGMA optimize;", &[])?;
    Ok(())
}

fn archive_channel(
    db: &rusqlite::Connection,
    client: &slack::requests::Client,
    token: &str,
    channel_id: &str,
) -> Result<(), Error> {
    // page forward starting from last saved ts
    let mut oldest_ts = match get_last_ts(db, channel_id)? {
        // first run: force slack to start from the oldest results
        None => 1,
        // later runs: start from last saved msg ts - edit window
        Some(ts) => ts - (EDIT_WINDOW_MINUTES * 60 * 1_000_000),
    };

    loop {
        println!("query from: {:?}", oldest_ts);
        let response = slack::channels::history(
            client,
            &token,
            &slack::channels::HistoryRequest {
                oldest: Some(&unix_micros_to_slack_ts(oldest_ts)),
                latest: None,
                channel: channel_id,
                count: Some(PAGE_SIZE),
                ..slack::channels::HistoryRequest::default()
            },
        )?;

        if let Some(messages) = response.messages {
            println!("Got {} messages", messages.len());
            if messages.len() == 0 {
                break;
            }

            // messages are returned in desc time order.
            // use the latest message timestamp as the starting point
            // for the next pagination query.
            if let Some(ts) = message_ts(&messages[0]) {
                oldest_ts = ts;
            }

            // iterate through messages in asc time order
            for message in messages.into_iter().rev() {
                match message {
                    slack::Message::Standard(msg) => {
                        db.execute(
                            "
                            INSERT OR REPLACE INTO message (`channel_id`, `ts`, `from`, `text`)
                            VALUES (?1, ?2, ?3, ?4)
                                ",
                            &[
                                &channel_id,
                                &slack_ts_to_unix_micros(&msg.ts.unwrap()),
                                &msg.user,
                                &msg.text,
                            ],
                        )?;
                    }
                    _ => continue, // skip over non-standard messages
                }
            }
        }

        if !response.has_more.unwrap_or(false) {
            // reached last page
            break;
        }
    }
    Ok(())
}

fn slack_ts_to_unix_micros(ts: &str) -> i64 {
    let (seconds, micros) = ts.split_at(10);
    (seconds.parse::<i64>().unwrap() * 1_000_000) + micros[1..].parse::<i64>().unwrap()
}

fn unix_micros_to_slack_ts(micros: i64) -> String {
    let (seconds, micros) = (micros / 1_000_000, micros % 1_000_000);
    format!("{:010}.{:06}", seconds, micros)
}

pub fn init_db(path: &str) -> Result<rusqlite::Connection, Error> {
    let db = rusqlite::Connection::open(path)?;
    println!("{:?}", db);

    db.execute(
        "
        CREATE TABLE IF NOT EXISTS `message` (
            `channel_id` TEXT NOT NULL,
            `ts` INTEGER NOT NULL,
            `from` TEXT NOT NULL,
            `text` BLOB,
            PRIMARY KEY(`channel_id`, `ts`)
        )",
        &[],
    )?;

    // sqlite can use skip-scan optimization for ts range queries
    // without a channel_id filter.
    db.execute(
        "
        CREATE INDEX IF NOT EXISTS `message_idx`
        ON `message` (channel_id, ts)
        ",
        &[],
    )?;

    Ok(db)
}

fn get_last_ts(db: &rusqlite::Connection, channel_id: &str) -> Result<Option<i64>, Error> {
    match db.query_row(
        "SELECT ts FROM message where channel_id = ? ORDER BY ts DESC LIMIT 1",
        &[&channel_id],
        |row| row.get_checked(0),
    ) {
        Ok(s) => Ok(Some(s.context("failed to get column value")?)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn message_ts(message: &slack::Message) -> Option<i64> {
    // this is exhausting
    let msg_ts = match *message {
        slack::Message::BotMessage(ref msg) => &msg.ts,
        slack::Message::ChannelArchive(ref msg) => &msg.ts,
        slack::Message::ChannelJoin(ref msg) => &msg.ts,
        slack::Message::ChannelLeave(ref msg) => &msg.ts,
        slack::Message::ChannelName(ref msg) => &msg.ts,
        slack::Message::ChannelPurpose(ref msg) => &msg.ts,
        slack::Message::ChannelTopic(ref msg) => &msg.ts,
        slack::Message::ChannelUnarchive(ref msg) => &msg.ts,
        slack::Message::FileComment(ref msg) => &msg.ts,
        slack::Message::FileMention(ref msg) => &msg.ts,
        slack::Message::FileShare(ref msg) => &msg.ts,
        slack::Message::GroupArchive(ref msg) => &msg.ts,
        slack::Message::GroupJoin(ref msg) => &msg.ts,
        slack::Message::GroupLeave(ref msg) => &msg.ts,
        slack::Message::GroupName(ref msg) => &msg.ts,
        slack::Message::GroupPurpose(ref msg) => &msg.ts,
        slack::Message::GroupTopic(ref msg) => &msg.ts,
        slack::Message::GroupUnarchive(ref msg) => &msg.ts,
        slack::Message::MeMessage(ref msg) => &msg.ts,
        slack::Message::MessageChanged(ref msg) => &msg.ts,
        slack::Message::MessageDeleted(ref msg) => &msg.ts,
        slack::Message::MessageReplied(ref msg) => &msg.ts,
        slack::Message::PinnedItem(ref msg) => &msg.ts,
        slack::Message::ReplyBroadcast(ref msg) => &msg.ts,
        slack::Message::Standard(ref msg) => &msg.ts,
        slack::Message::UnpinnedItem(ref msg) => &msg.ts,
    };
    match *msg_ts {
        Some(ref ts_str) => Some(slack_ts_to_unix_micros(&ts_str)),
        _ => None,
    }
}
