import java.io.*;
import java.nio.charset.StandardCharsets;
import org.xerial.snappy.Snappy;

public class TestCompression {
    public static void main(String[] args) {
        try {
            // Test data
            String testMessage = "{\"event\":\"login\", \"user\":\"test123\"}";
            byte[] original = testMessage.getBytes(StandardCharsets.UTF_8);

            System.out.println("Original message: " + testMessage);
            System.out.println("Original size: " + original.length + " bytes");

            // Compress with Snappy
            byte[] compressed = compress(original);
            System.out.println("Compressed size: " + compressed.length + " bytes");
            System.out
                    .println("Compression ratio: " + (1.0 - (double) compressed.length / original.length) * 100 + "%");

            // Decompress
            byte[] decompressed = decompress(compressed);
            String result = new String(decompressed, StandardCharsets.UTF_8);

            System.out.println("Decompressed message: " + result);
            System.out.println("Round-trip successful: " + testMessage.equals(result));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static byte[] compress(byte[] raw) {
        try {
            return Snappy.compress(raw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] decompress(byte[] compressed) {
        try {
            return Snappy.uncompress(compressed);
        } catch (IOException e) {
            System.err.println("Snappy decompression failed: " + e.getMessage());
            return compressed;
        }
    }
}