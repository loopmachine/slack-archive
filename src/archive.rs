use std::env;
use failure::Error;
use slack;

pub fn archive() -> Result<(), Error> {
    let token = env::var("SLACK_API_TOKEN").expect("SLACK_API_TOKEN not set.");
    let client = slack::default_client().unwrap();

    let response =
        slack::channels::list(&client, &token, &slack::channels::ListRequest::default())?;

    if let Some(channels) = response.channels {
        for channel in channels {
            if let (Some(id), Some(name)) = (channel.id, channel.name) {
                println!("Archiving channel: {} ({})", name, id);
                if name != "general" {
                    continue;
                }
                let messages = archive_channel(&client, &token, &id)?;
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
) -> Result<(), Error> {
    loop {
        let response = slack::channels::history(
            client,
            &token,
            &slack::channels::HistoryRequest {
                channel: channel_id,
                count: Some(10),
                ..slack::channels::HistoryRequest::default()
            },
        )?;
        if let Some(messages) = response.messages {
            println!("Got {} messages:", messages.len());
            for message in messages {
                println!("{:?}", message);
            }
        }
        break;
    }
    Ok(())
}
