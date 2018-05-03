use std::env;

use failure::{Error, ResultExt};
use slack;
use rusqlite;

const PAGE_SIZE: u32 = 1000; // max allowed by slack api

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
        None => Some("1".to_string()),
        // later runs: start from last saved msg ts
        ts @ _ => ts,
    };

    loop {
        println!("query from: {:?}", oldest_ts);
        let response = slack::channels::history(
            client,
            &token,
            &slack::channels::HistoryRequest {
                oldest: oldest_ts.as_ref().map(String::as_str),
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
            // for the next page query.
            oldest_ts = message_ts(&messages[0]).clone();

            // iterate through messages in asc time order
            for message in messages.into_iter().rev() {
                match message {
                    slack::Message::Standard(msg) => {
                        db.execute(
                            "
                            INSERT INTO message (`channel_id`, `ts`, `from`, `text`)
                            VALUES (?1, ?2, ?3, ?4)
                                ",
                            &[&channel_id, &msg.ts, &msg.user, &msg.text],
                        )?;
                    }
                    _ => continue, // skip over other message types
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

pub fn init_db(path: &str) -> Result<rusqlite::Connection, Error> {
    let db = rusqlite::Connection::open(path)?;
    println!("{:?}", db);

    db.execute(
        "
        CREATE TABLE IF NOT EXISTS `message` (
            `id` INTEGER PRIMARY KEY,
            `channel_id` TEXT NOT NULL,
            `ts` TEXT NOT NULL,
            `from` TEXT NOT NULL,
            `text` BLOB
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

fn get_last_ts(db: &rusqlite::Connection, channel_id: &str) -> Result<Option<String>, Error> {
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

fn message_ts(message: &slack::Message) -> &Option<String> {
    // this is exhausting
    match *message {
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
    }
}
