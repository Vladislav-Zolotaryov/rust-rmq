use std::{thread, time::Duration};

use amiquip::{Connection, Exchange, Publish, Result, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};

fn main() -> Result<()> {
    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Get a handle to the direct exchange on our channel.
    let exchange = Exchange::direct(&channel);

    // Publish a message to the "hello" queue.
    exchange.publish(Publish::new("hello there".as_bytes(), "hello"))?;

    thread::spawn(move || -> Result<()> {
        // Instead, declare once the channel is moved into this thread.
        let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
        let consumer = queue.consume(ConsumerOptions::default())?;
        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
                    println!("Received [{}]", body);
                    consumer.ack(delivery)?;
                }
                other => {
                    println!("Consumer ended: {:?}", other);
                    break;
                }
            }        
        }
        Ok(())
    });

    thread::sleep(Duration::from_secs(5));

    connection.close()
}