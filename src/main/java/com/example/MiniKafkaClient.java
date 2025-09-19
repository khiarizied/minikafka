package com.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

/**
 * Stand-alone TCP client for MiniKafka.
 * - produce(topic, key, bytes) -> offset
 * - consume(group, topic, Handler) -> long-running listener thread
 * Works from ANY JVM, no shared classpath with server.
 */
public class MiniKafkaClient implements Closeable {

    public interface Handler {
        /**
         * @param partition logical partition index
         * @param offset    byte offset in that partition file
         * @param key       optional key (may be empty)
         * @param payload   de-compressed bytes
         */
        void onMessage(int partition, long offset, String key, byte[] payload) throws Exception;
    }

    private final String host;
    private final int port;
    private final ExecutorService listenerPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mini-kafka-listener");
        t.setDaemon(true);
        return t;
    });

    public MiniKafkaClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /*
     * ----------------------------------------------------------
     * PRODUCE
     * ----------------------------------------------------------
     */
    public long produce(String topic, String key, byte[] payload) throws IOException {
        try (Socket s = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                DataInputStream in = new DataInputStream(s.getInputStream())) {

            String frame = "TOPIC" + topic + '<' + (key == null ? "" : key) + '|'
                    + new String(payload, StandardCharsets.UTF_8) + ">\n";
            out.writeBytes(frame);
            out.flush();

            String resp = readLine(in);
            if (!resp.startsWith("+OFFSET "))
                throw new IOException("Server error: " + resp);
            return Long.parseLong(resp.split(" ")[1]);
        }
    }

    /*
     * ----------------------------------------------------------
     * CONSUME (long-lived listener)
     * ----------------------------------------------------------
     */
    public void consume(String group, String topic, Handler handler) {
        listenerPool.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket s = new Socket(host, port);
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                        DataInputStream in = new DataInputStream(s.getInputStream())) {

                    // 1. ask for next message
                    out.writeBytes(topic + ':' + group + '\n');
                    out.flush();

                    String resp = readLine(in);
                    if (resp.startsWith("+EMPTY")) {
                        Thread.sleep(200); // simple poll back-off
                        continue;
                    }
                    if (!resp.startsWith("+MSG "))
                        throw new IOException("Unexpected response: " + resp);

                    String[] meta = resp.split(" ");
                    long offset = Long.parseLong(meta[1]);
                    int len = Integer.parseInt(meta[2]);

                    byte[] compressed = new byte[len];
                    in.readFully(compressed);
                    in.read(); // consume trailing \n

                    byte[] payload = decompress(compressed);

                    // 2. deliver to user code
                    int partition = guessPartition(topic, compressed); // server does not send it
                    handler.onMessage(partition, offset, "", payload); // key not sent either (optional)

                    // 3. ack
                    out.writeBytes("ACK\n");
                    out.flush();
                    String ackResp = readLine(in);
                    if (!"+OK".equals(ackResp))
                        throw new IOException("Ack failed: " + ackResp);

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("Listener error, will retry: " + e.getMessage());
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    /*
     * ----------------------------------------------------------
     * UTILS
     * ----------------------------------------------------------
     */
    private static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1 && b != '\n')
            sb.append((char) b);
        return sb.toString();
    }

    private static byte[] decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
                ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = gzip.read(buf)) != -1)
                baos.write(buf, 0, n);
            return baos.toByteArray();
        }
    }

    private int guessPartition(String topic, byte[] compressed) {
        // Server uses key-hash – we don't have key here.
        // Quick workaround: use payload hash
        return Math.abs(Arrays.hashCode(compressed)) % 4; // assume 4 partitions
    }

    @Override
    public void close() {
        listenerPool.shutdownNow();
    }

    /*
     * ----------------------------------------------------------
     * DEMO
     * ----------------------------------------------------------
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MiniKafkaClient <host> <port> [topic] [group]");
            System.exit(1);
        }
        
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args.length > 2 ? args[2] : "orders";
        String group = args.length > 3 ? args[3] : "g1";
        
        MiniKafkaClient cli = new MiniKafkaClient(host, port);

        // 1. fire-and-forget producer
        long off = cli.produce(topic, "user123", "{\"event\":\"login\"}".getBytes(StandardCharsets.UTF_8));
        System.out.println("Sent to offset " + off);

        // 2. continuous consumer
        cli.consume(group, topic, (p, o, k, v) -> {
            System.out.printf("Received: partition=%d offset=%d key=%s payload=%s%n",
                    p, o, k, new String(v, StandardCharsets.UTF_8));
        });

        // keep alive
        Thread.sleep(Long.MAX_VALUE);
        cli.close();
    }
}