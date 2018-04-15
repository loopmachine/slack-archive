use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};

use failure::Error;
use slack;
use walkdir::WalkDir;
use chrono::Utc;

pub fn archive() -> Result<(), Error> {
    let token = env::var("SLACK_API_TOKEN").expect("SLACK_API_TOKEN not set.");
    let data_dir = env::var("DATA_DIR").unwrap_or("./data".to_string());
    let client = slack::default_client().unwrap();

    let response =
        slack::channels::list(&client, &token, &slack::channels::ListRequest::default())?;

    if let Some(channels) = response.channels {
        for channel in channels {
            if let (Some(id), Some(name)) = (channel.id, channel.name) {
                if name != "general" {
                    continue;
                }

                let channel_dir = format!("{}/channels/{}", data_dir, name);
                fs::create_dir_all(&channel_dir)?;

                println!("Archiving channel: {} ({})", name, id);

                let messages = archive_channel(&client, &token, &id, &channel_dir)?;
                println!("{:?}", messages);
            }
        }
    }
    Ok(())
}

fn archive_channel(
    client: &slack::requests::Client,
    token: &str,
    channel_id: &str,
    channel_dir: &str,
) -> Result<(), Error> {
    loop {
        let last_ts = read_last_ts(channel_dir)?;
        if let Some(ref ts) = last_ts {
            println!("Getting messages since last run: {:}", ts);
        } else {
            println!("Getting all messages");
        }

        let response = slack::channels::history(
            client,
            &token,
            &slack::channels::HistoryRequest {
                oldest: last_ts.as_ref().map(String::as_str),
                channel: channel_id,
                count: Some(10),
                ..slack::channels::HistoryRequest::default()
            },
        )?;

        let now = Utc::now();
        let file_path = format!(
            "{}/{:10}.{:06}",
            channel_dir,
            now.timestamp(),
            now.timestamp_subsec_micros()
        );
        println!("{}", file_path);
        let f = File::create(&file_path)?;
        let mut writer = BufWriter::new(f);

        if let Some(messages) = response.messages {
            println!("Got {} messages:", messages.len());
            for message in messages {
                println!("{:?}", message);
                match message {
                    slack::Message::Standard(msg) => {
                        if let Some(text) = msg.text {
                            writeln!(&mut writer, "{}", text)?;
                        }
                    }
                    _ => continue,
                }
            }
        }
        break;
    }
    Ok(())
}

fn read_last_ts(channel_dir: &str) -> Result<Option<String>, Error> {
    match WalkDir::new(channel_dir)
        .min_depth(1)
        .max_depth(1)
        .sort_by(|a, b| b.file_name().cmp(a.file_name()))
        .into_iter()
        .filter_entry(|e| e.file_type().is_file())
        .next()
    {
        Some(entry) => Ok(Some(entry?.file_name().to_string_lossy().into_owned())),
        None => Ok(None),
    }
}
