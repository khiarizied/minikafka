package com.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A test producer that continuously sends messages to MiniKafka
 * to test the listener functionality.
 */
public class MessageProducer {
    private final MiniKafkaClient client;
    private final String topic;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private volatile boolean running = true;

    public MessageProducer(String host, int port, String topic) {
        this.client = new MiniKafkaClient(host, port);
        this.topic = topic;
    }

    public void start() {
        System.out.println("Starting message producer for topic: " + topic);

        // Send messages every 2 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!running) return;

                // Generate a random message
                int messageId = random.nextInt(1000);
                String key = "key-" + messageId;
                String payload = "{\"id\":" + messageId + ",\"timestamp\":" + System.currentTimeMillis() + ",\"data\":\"Test message " + messageId + "\"}";

                // Send the message
                long offset = client.produce(topic, key, payload.getBytes(StandardCharsets.UTF_8));
                System.out.println("Sent message with ID " + messageId + " to offset " + offset);
            } catch (IOException e) {
                System.err.println("Error producing message: " + e.getMessage());
            }
        }, 0, 2, TimeUnit.SECONDS);
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
            System.err.println("Usage: java MessageProducer <host> <port> <topic>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];

        MessageProducer producer = new MessageProducer(host, port, topic);
        producer.start();

        // Add shutdown hook to gracefully stop the producer
        Runtime.getRuntime().addShutdownHook(new Thread(producer::stop));

        // Keep running until interrupted
        Thread.sleep(Long.MAX_VALUE);
    }
}
