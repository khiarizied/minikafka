package com.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test producer that sends bulk messages to MiniKafka every second
 * to test the listener functionality and performance.
 */
public class MessageProducer {
    private final MiniKafkaClient client;
    private final String topic;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private volatile boolean running = true;
    private final int messagesPerSecond;
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private long lastReportTime = System.currentTimeMillis();

    public MessageProducer(String host, int port, String topic, int messagesPerSecond) {
        this.client = new MiniKafkaClient(host, port);
        this.topic = topic;
        this.messagesPerSecond = messagesPerSecond;
    }

    public void start() {
        System.out.println("Starting message producer for topic: " + topic + 
                          ", sending " + messagesPerSecond + " messages per second");

        // Send bulk messages every second
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!running)
                    return;

                long startTime = System.currentTimeMillis();
                int sentInBatch = 0;
                
                // Send multiple messages in this batch
                for (int i = 0; i < messagesPerSecond; i++) {
                    // Generate a random message
                    int messageId = random.nextInt(1000000);
                    String key = "key-" + messageId;
                    String payload = "{\"id\":" + messageId + 
                            ",\"timestamp\":" + System.currentTimeMillis() +
                            ",\"batch\":\"batch-" + (totalMessagesSent.get() / messagesPerSecond) +
                            ",\"data\":\"Test message " + messageId + " from batch " + (totalMessagesSent.get() / messagesPerSecond) + "\"}";

                    // Send the message
                    long offset = client.produce(topic, key, payload.getBytes(StandardCharsets.UTF_8));
                    totalMessagesSent.incrementAndGet();
                    sentInBatch++;
                }
                
                long batchTime = System.currentTimeMillis() - startTime;
                
                // Report statistics every 10 seconds
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastReportTime >= 10000) {
                    double secondsElapsed = (currentTime - lastReportTime) / 1000.0;
                    double rate = totalMessagesSent.get() / secondsElapsed;
                    System.out.printf("Sent %d messages in %.2f seconds (%.2f msgs/sec)%n", 
                            totalMessagesSent.get(), secondsElapsed, rate);
                    totalMessagesSent.set(0);
                    lastReportTime = currentTime;
                }
                
                if (batchTime > 1000) {
                    System.out.println("Warning: Last batch took " + batchTime + "ms to send " + sentInBatch + " messages");
                }
            } catch (IOException e) {
                System.err.println("Error producing messages: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        client.close();
        System.out.println("Message producer stopped");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java MessageProducer <host> <port> <topic> [messagesPerSecond]");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int messagesPerSecond = args.length > 3 ? Integer.parseInt(args[3]) : 10;

        MessageProducer producer = new MessageProducer(host, port, topic, messagesPerSecond);
        producer.start();

        // Add shutdown hook to gracefully stop the producer
        Runtime.getRuntime().addShutdownHook(new Thread(producer::stop));

        // Keep running until interrupted
        Thread.sleep(Long.MAX_VALUE);
    }
}
