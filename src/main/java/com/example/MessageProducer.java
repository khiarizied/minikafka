package com.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test producer that sends bulk messages to MiniKafka every second
 * to test the listener functionality and performance.
 */
public class MessageProducer {

    public void start(String topic, int messagesPerSecond, int durationSeconds) {
        System.out.println("Starting producer for topic '" + topic + "'...");
        
        // The client is now stateless regarding connection, just holds config.
        MiniKafkaClientWithOffset client = new MiniKafkaClientWithOffset("localhost", 9092);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        AtomicLong totalMessagesSent = new AtomicLong(0);
        Random random = new Random();

        Runnable messageSender = () -> {
            try {
                int sentInBatch = 0;
                // Send multiple messages in this batch
                for (int i = 0; i < messagesPerSecond; i++) {
                    int messageId = random.nextInt(1000000);
                    String key = "key-" + messageId;

                    // Generate a JSON text payload
                    String payload = "{\"id\":" + messageId + ",\"ts\":" + System.currentTimeMillis() + "}";
                    
                    // Send the message
                    client.produce(topic, key, payload.getBytes(StandardCharsets.UTF_8));
                    totalMessagesSent.incrementAndGet();
                    sentInBatch++;
                }
                System.out.println("Sent " + sentInBatch + " messages in this batch. Total sent: " + totalMessagesSent.get());
            } catch (IOException e) {
                System.err.println("Failed to send message: " + e.getMessage());
                // Optional: add a stack trace for debugging
                // e.printStackTrace();
            }
        };

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(messageSender, 0, 1, TimeUnit.SECONDS);

        try {
            // Let the producer run for the specified duration
            future.get(durationSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // This is the normal way to stop after the duration
            future.cancel(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
            
            // Close the client's thread pool
            client.close();
        }

        System.out.println("Producer finished. Total messages sent: " + totalMessagesSent.get());
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java com.example.MessageProducer <topic> [messagesPerSecond] [durationSeconds]");
            System.exit(1);
        }

        String topic = args[0];
        int messagesPerSecond = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int durationSeconds = args.length > 2 ? Integer.parseInt(args[2]) : 10;

        MessageProducer producer = new MessageProducer();
        producer.start(topic, messagesPerSecond, durationSeconds);
    }
}
