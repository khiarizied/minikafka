package com.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.*;

/**
 * Tiny single-node TCP log server with
 * - partitions
 * - gzip compression
 * - pluggable per-partition listeners
 */
public class MiniKafka {

    private final Path base;
    private final int partitionCount;
    private final ServerSocket server;
    private final Map<String, RandomAccessFile[]> topicFiles = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> offsets = new ConcurrentHashMap<>(); // topic:partition -> group ->
                                                                                      // offset
    private final Path offsetFile;
    private final Map<String, List<PartitionListener>> listeners = new ConcurrentHashMap<>();

    public interface PartitionListener {
        void onMessage(int partition, long offset, String key, byte[] payload);
    }

    public MiniKafka(int port, Path base, int partitions) throws IOException {
        this.base = base;
        this.partitionCount = partitions;
        this.offsetFile = base.resolve("offsets.json");
        Files.createDirectories(base.resolve("logs"));
        loadOffsets();
        server = new ServerSocket(port);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /* --------------- public listener API --------------- */
    public synchronized void addListener(String topic, PartitionListener l) {
        listeners.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(l);
    }

    /* --------------- server main loop --------------- */
    public void serve() throws Exception {
        System.out.println("Listening on " + server.getLocalPort() + " with " + partitionCount + " partitions");
        while (true) {
            Socket s = server.accept();
            new Thread(new Session(s)).start();
        }
    }

    /* --------------- persistence helpers --------------- */
    private RandomAccessFile[] topicFiles(String topic) {
        return topicFiles.computeIfAbsent(topic, t -> {
            try {
                RandomAccessFile[] fa = new RandomAccessFile[partitionCount];
                for (int p = 0; p < partitionCount; p++)
                    fa[p] = new RandomAccessFile(base.resolve("logs").resolve(t + "-" + p + ".log").toFile(), "rw");
                return fa;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!Files.exists(offsetFile))
            return;
        try (Reader r = Files.newBufferedReader(offsetFile)) {
            Map<String, Map<String, Double>> raw = new com.google.gson.Gson().fromJson(r, Map.class);
            raw.forEach((tp, gm) -> gm.forEach((g, off) -> offsets.computeIfAbsent(tp, k -> new ConcurrentHashMap<>())
                    .put(g, off.longValue())));
        } catch (Exception e) {
            System.err.println("offset load failed: " + e);
        }
    }

    private void saveOffsets() {
        try (Writer w = Files.newBufferedWriter(offsetFile)) {
            new com.google.gson.Gson().toJson(offsets, w);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void shutdown() {
        try {
            server.close();
            topicFiles.values().forEach(fa -> {
                for (RandomAccessFile f : fa)
                    try {
                        f.close();
                    } catch (Exception ignore) {
                    }
            });
            saveOffsets();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* --------------- per-client session --------------- */
    private class Session implements Runnable {
        private final Socket sock;
        private final DataInputStream in;
        private final DataOutputStream out;

        private String consumeTopic, consumeGroup;
        private int consumePartition;
        private long deliverOffset;
        private byte[] deliverBuf;

        Session(Socket s) throws IOException {
            this.sock = s;
            this.in = new DataInputStream(s.getInputStream());
            this.out = new DataOutputStream(s.getOutputStream());
        }

        public void run() {
            try {
                String line;
                while ((line = readLine()) != null) {
                    if (line.startsWith("TOPIC")) {
                        handleProduce(line.substring(5));
                    } else if (line.contains(":")) {
                        handleConsume(line);
                    } else if (line.equals("ACK")) {
                        handleAck();
                    } else {
                        write("-ERR unknown command\n");
                    }
                }
            } catch (Exception e) {
                System.err.println("client error: " + e);
            } finally {
                try {
                    sock.close();
                } catch (IOException ignore) {
                }
            }
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = in.read()) != -1 && b != '\n')
                sb.append((char) b);
            return b == -1 ? null : sb.toString();
        }

        private void write(String s) throws IOException {
            out.writeBytes(s);
            out.flush();
        }

        /* ---------- PRODUCE ---------- */
        private void handleProduce(String topicKeyPayload) throws IOException {
            int k = topicKeyPayload.indexOf('<');
            if (k < 0) {
                write("-ERR bad format, expected TOPIC<key|payload>\n");
                return;
            }
            String topic = topicKeyPayload.substring(0, k);
            String keyAndPay = topicKeyPayload.substring(k + 1, topicKeyPayload.length() - 1);
            int sep = keyAndPay.indexOf('|');
            String key = sep < 0 ? "" : keyAndPay.substring(0, sep);
            byte[] payloadRaw = (sep < 0 ? keyAndPay : keyAndPay.substring(sep + 1)).getBytes(StandardCharsets.UTF_8);

            int partition = Math.abs(key.hashCode()) % partitionCount;
            RandomAccessFile file = topicFiles(topic)[partition];

            byte[] compressed;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(payloadRaw);
                gzip.finish();
                compressed = baos.toByteArray();
            }

            long offset;
            synchronized (file) {
                offset = file.length();
                file.seek(offset);
                file.writeInt(compressed.length);
                file.write(compressed);
            }
            write("+OFFSET " + offset + '\n');

            /* notify listeners */
            byte[] decompressed = decompress(compressed);
            List<PartitionListener> list = listeners.get(topic);
            if (list != null) {
                for (PartitionListener l : list)
                    l.onMessage(partition, offset, key, decompressed);
            }
        }

        /* ---------- CONSUME ---------- */
        private void handleConsume(String req) throws IOException {
            String[] parts = req.split(":");
            if (parts.length != 2) {
                write("-ERR bad format, expected TOPIC:GROUP\n");
                return;
            }
            consumeTopic = parts[0];
            consumeGroup = parts[1];
            RandomAccessFile[] files = topicFiles(consumeTopic);

            /* find first partition with data */
            for (int p = 0; p < partitionCount; p++) {
                String tpKey = consumeTopic + ":" + p;
                long next = offsets.computeIfAbsent(tpKey, k -> new ConcurrentHashMap<>())
                        .getOrDefault(consumeGroup, 0L);
                RandomAccessFile f = files[p];
                synchronized (f) {
                    if (next >= f.length())
                        continue;
                    f.seek(next);
                    int len = f.readInt();
                    byte[] compressed = new byte[len];
                    f.readFully(compressed);
                    deliverBuf = decompress(compressed); // 保留解压后的数据用于其他用途
                    deliverOffset = next;
                    consumePartition = p;
                    write("+MSG " + deliverOffset + ' ' + compressed.length + '\n');
                    out.write(compressed); // 发送压缩后的数据
                    out.write('\n');
                    out.flush();
                    return;
                }
            }
            write("+EMPTY\n");
        }

        private void handleAck() throws IOException {
            if (consumeTopic == null) {
                write("-ERR no outstanding msg\n");
                return;
            }
            String tpKey = consumeTopic + ":" + consumePartition;
            offsets.get(tpKey).put(consumeGroup, deliverOffset + 4 + compress(deliverBuf).length);
            saveOffsets();
            write("+OK\n");
            consumeTopic = null;
        }
    }

    /* --------------- compression helpers --------------- */
    private static byte[] compress(byte[] raw) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(raw);
            gzip.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] decompress(byte[] compressed) {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
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

    /* --------------- client helper (listener) --------------- */
    public static class Client {
        private final String host;
        private final int port;
        private final MiniKafka serverLocal; // null when remote

        private Client(String host, int port, MiniKafka serverLocal) {
            this.host = host;
            this.port = port;
            this.serverLocal = serverLocal;
        }

        public void addListener(String topic, PartitionListener l) {
            if (serverLocal != null) {
                serverLocal.addListener(topic, l);
                return;
            }
            throw new UnsupportedOperationException("remote listener not implemented over TCP yet");
        }

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
    }

    public static Client connect(String host, int port) {
        return new Client(host, port, null);
    }

    public static Client connectLocal(MiniKafka server) {
        return new Client(null, 0, server);
    }

    /* --------------- main --------------- */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: java MiniKafka <port> <data-dir> [partitionCount]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        int parts = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        new MiniKafka(port, dir, parts).serve();
    }
}