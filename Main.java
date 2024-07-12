package application;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        try {
            // Set up the storage
            String storageDirectory = "benchmarkStorage";
            int binCount = 100;
            boolean enableParity = true;
            int parityGroupCount = 10;
            
            System.out.println("Cleaning up storage directory...");
            // Clean up the storage directory before benchmarking
            if (Files.exists(Paths.get(storageDirectory))) {
                deleteDirectory(Paths.get(storageDirectory));
            }
            
            System.out.println("Initializing SimpleKeyValueStorage...");
            SimpleKeyValueStorage storage = new SimpleKeyValueStorage(storageDirectory, binCount, enableParity, parityGroupCount);

            // Set up test data
            int numberOfEntries = 10000; // Large number of entries
            int numberOfReads = 100; // Number of reads to perform
            int numberOfWildcardSearches = 100; // Number of wildcard searches to perform

            System.out.println("Generating test data...");
            // Generate test data
            HashMap<String, String> testData = generateTestData(numberOfEntries);
            List<String> keys = new ArrayList<>(testData.keySet());

            // Write Benchmark
            System.out.println("Starting write benchmark...");
            long writeStartTime = System.currentTimeMillis();
            storage.set(testData);
            storage.sync();
            long writeEndTime = System.currentTimeMillis();
            long writeDuration = writeEndTime - writeStartTime;
            double writeRate = numberOfEntries / (writeDuration / 1000.0);
            System.out.println("Write Duration: " + writeDuration + " ms");
            System.out.println("Write Rate: " + writeRate + " entries/s");

            // Read Benchmark
            System.out.println("Starting read benchmark...");
            long readStartTime = System.currentTimeMillis();
            for (int i = 0; i < numberOfReads; i++) {
                List<String> keysToRead = selectRandomKeys(keys, 100); // Read in batches of 100 keys
                storage.get(keysToRead);
            }
            long readEndTime = System.currentTimeMillis();
            long readDuration = readEndTime - readStartTime;
            double readRate = (numberOfReads * 100) / (readDuration / 1000.0); // total number of read operations
            System.out.println("Read Duration: " + readDuration + " ms");
            System.out.println("Read Rate: " + readRate + " keys/s");

            // Wildcard Search Benchmark
            System.out.println("Generating wildcard search keys...");
            List<String> wildcardKeys = generateWildcardKeys(keys, numberOfWildcardSearches);
            System.out.println("Starting wildcard search benchmark...");
            long wildcardSearchStartTime = System.currentTimeMillis();
            for (String wildcardKey : wildcardKeys) {
                List<String> wildcardList = new ArrayList<>();
                wildcardList.add(wildcardKey);
                storage.get(wildcardList);
            }
            long wildcardSearchEndTime = System.currentTimeMillis();
            long wildcardSearchDuration = wildcardSearchEndTime - wildcardSearchStartTime;
            double wildcardSearchRate = numberOfWildcardSearches / (wildcardSearchDuration / 1000.0);
            System.out.println("Wildcard Search Duration: " + wildcardSearchDuration + " ms");
            System.out.println("Wildcard Search Rate: " + wildcardSearchRate + " searches/s");

            System.out.println("BENCHMARK DONE");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HashMap<String, String> generateTestData(int numberOfEntries) {
        HashMap<String, String> testData = new HashMap<>();
        for (int i = 0; i < numberOfEntries; i++) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            testData.put(key, value);
        }
        return testData;
    }

    private static List<String> selectRandomKeys(List<String> keys, int numberOfKeys) {
        Random random = new Random();
        List<String> selectedKeys = new ArrayList<>();
        for (int i = 0; i < numberOfKeys; i++) {
            selectedKeys.add(keys.get(random.nextInt(keys.size())));
        }
        return selectedKeys;
    }

    private static List<String> generateWildcardKeys(List<String> keys, int numberOfWildcardSearches) {
        Random random = new Random();
        List<String> wildcardKeys = new ArrayList<>();
        for (int i = 0; i < numberOfWildcardSearches; i++) {
            String key = keys.get(random.nextInt(keys.size()));
            // Replace random character with wildcard character
            char[] keyChars = key.toCharArray();
            int pos = random.nextInt(keyChars.length);
            keyChars[pos] = '*';
            wildcardKeys.add(new String(keyChars));
        }
        return wildcardKeys;
    }

    private static void deleteDirectory(java.nio.file.Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (java.util.stream.Stream<java.nio.file.Path> entries = Files.list(path)) {
                for (java.nio.file.Path entry : (Iterable<java.nio.file.Path>) entries::iterator) {
                    deleteDirectory(entry);
                }
            }
        }
        Files.delete(path);
    }
}
