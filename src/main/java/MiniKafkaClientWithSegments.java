package com.example;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.function.BiConsumer;

/**
 * Client for MiniKafka with segmentation support.
 */
public class MiniKafkaClientWithSegments implements AutoCloseable {
    private final String host;
    private final int port;
    private volatile boolean running = true;

    public MiniKafkaClientWithSegments(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Produce a message to a topic.
     * @param topic Topic name
     * @param key Message key
     * @param payload Message payload
     * @return Offset of the message
     * @throws IOException If an I/O error occurs
     */
    public long produce(String topic, String key, byte[] payload) throws IOException {
        try (Socket s = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(s.getOutputStream());
             DataInputStream in = new DataInputStream(s.getInputStream())) {
            out.writeBytes("TOPIC" + topic + '<' + key + '|' + new String(payload, StandardCharsets.UTF_8) + ">\n");
            out.flush();
            String resp = new Scanner(in).nextLine();
            return Long.parseLong(resp.split(" ")[1]);
        }
    }

    /**
     * Consume messages from a topic.
     * @param group Consumer group
     * @param topic Topic name
     * @param callback Callback to handle messages
     * @throws IOException If an I/O error occurs
     */
    public void consume(String group, String topic, BiConsumer<Integer, Long> callback) throws IOException {
        new Thread(() -> {
            try (Socket s = new Socket(host, port);
                 DataOutputStream out = new DataOutputStream(s.getOutputStream());
                 DataInputStream in = new DataInputStream(s.getInputStream())) {

                while (running) {
                    // Request messages
                    out.writeBytes(topic + ":" + group + "\n");
                    out.flush();

                    // Read response
                    String line = readLine(in);
                    if (line == null) break;

                    if (line.startsWith("+MSG ")) {
                        String[] parts = line.split(" ");
                        long offset = Long.parseLong(parts[1]);
                        int length = Integer.parseInt(parts[2]);

                        // Read compressed payload
                        byte[] compressed = new byte[length];
                        in.readFully(compressed);
                        // Read newline
                        in.readByte();

                        // Decompress payload
                        byte[] payload = decompress(compressed);

                        // Call callback
                        callback.accept(-1, offset);  // -1 for partition (not tracked in this simple client)

                        // Send ACK
                        out.writeBytes("ACK\n");
                        out.flush();

                        // Read OK response
                        String ackResp = readLine(in);
                        if (!"+OK".equals(ackResp)) {
                            System.err.println("Unexpected ACK response: " + ackResp);
                        }
                    } else if ("+EMPTY".equals(line)) {
                        // No messages, wait a bit
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else {
                        System.err.println("Unexpected response: " + line);
                        break;
                    }
                }
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1 && b != '\n')
            sb.append((char) b);
        return b == -1 ? null : sb.toString();
    }

    private static byte[] decompress(byte[] compressed) {
        try (java.util.zip.GZIPInputStream gzip = new java.util.zip.GZIPInputStream(new ByteArrayInputStream(compressed));
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = gzip.read(buf)) != -1)
                baos.write(buf, 0, n);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        running = false;
    }

    /**
     * Example usage.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MiniKafkaClientWithSegments <host> <port> [topic] [group]");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args.length > 2 ? args[2] : "orders";
        String group = args.length > 3 ? args[3] : "g1";

        MiniKafkaClientWithSegments cli = new MiniKafkaClientWithSegments(host, port);

        // 1. fire-and-forget producer
        long off = cli.produce(topic, "user123", "{\"event\":\"login\"}".getBytes(StandardCharsets.UTF_8));
        System.out.println("Sent to offset " + off);

        // 2. continuous consumer
        cli.consume(group, topic, (partition, offset) -> {
            System.out.println("Received message at offset " + offset);
        });

        // keep alive
        Thread.sleep(Long.MAX_VALUE);
        cli.close();
    }
}
