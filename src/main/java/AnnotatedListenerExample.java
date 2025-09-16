package com.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Example class demonstrating the use of @KafkaListener annotations.
 * This class can be run as a standalone consumer.
 */
public class AnnotatedListenerExample {

    @KafkaListener(topic = "test-topic", groupId = "example-group")
    public void handleTestMessage(int partition, long offset, String key, byte[] payload) {
        System.out.println("Received test message: " + new String(payload, StandardCharsets.UTF_8));
    }

    @KafkaListener(topic = "orders", groupId = "order-processors")
    public void handleOrder(int partition, long offset, String key, byte[] payload) {
        System.out.println("Processing order: " + new String(payload, StandardCharsets.UTF_8));
        // Process order logic here
    }

    @KafkaListener(topic = "logs", groupId = "log-consumers")
    public void handleLog(int partition, long offset, String key, byte[] payload) {
        String logMessage = new String(payload, StandardCharsets.UTF_8);
        System.out.println("Log entry: " + logMessage);

        // Example of filtering logs
        if (logMessage.contains("ERROR")) {
            System.err.println("Found error in log: " + logMessage);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java AnnotatedListenerExample <host> <port>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        // Create client
        MiniKafkaClient client = new MiniKafkaClient(host, port);

        // Create processor
        KafkaListenerProcessor processor = new KafkaListenerProcessor(client);

        // Create and register listener
        AnnotatedListenerExample listener = new AnnotatedListenerExample();
        processor.process(listener);

        System.out.println("Annotated listener started. Press Ctrl+C to stop.");

        // Keep running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            client.close();
        }
    }
}
