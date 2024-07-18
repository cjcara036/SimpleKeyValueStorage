import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SimpleCache class provides a caching mechanism for integer keys and their associated HashMaps.
 * The cache content is synchronized periodically using a specified function.
 */
class SimpleCache {
    private static final String CACHE_EXTENSION = ".cache";
    private List<Integer> listOfCacheFiles;
    private HashMap<Integer, HashMap<String, String>> fileContentMap;
    private int cacheSize;
    private boolean objectIsBusy;
    private int updateCycleTimeSec;
    private Function<Integer, HashMap<String, String>> readCacheFileFunction;
    private ScheduledExecutorService scheduler;

    /**
     * Constructor initializes the SimpleCache with the given parameters.
     *
     * @param filePath             the path to the .cache file containing initial cache keys
     * @param objCacheSize         the maximum size of the cache
     * @param updateCycleTimeSec   the interval in seconds at which the cache content is synchronized
     * @param readCacheFileFunction a function that takes an integer key and returns a HashMap of cache content
     */
    SimpleCache(Path filePath, int objCacheSize, int updateCycleTimeSec, Function<Integer, HashMap<String, String>> readCacheFileFunction) {
        this.listOfCacheFiles = new ArrayList<>();
        this.fileContentMap = new HashMap<>();
        this.cacheSize = objCacheSize;
        this.objectIsBusy = false;
        this.updateCycleTimeSec = updateCycleTimeSec;
        this.readCacheFileFunction = readCacheFileFunction;

        // Check if the file exists, create if not
        if (Files.notExists(filePath)) {
            try {
                Files.createFile(filePath);
            } catch (IOException e) {
                System.err.println("Error creating the cache file: " + e.getMessage());
            }
        }
        
        if (filePath.toString().endsWith(CACHE_EXTENSION)) {
            try {
                Set<Integer> uniqueItems = new HashSet<>(Files.readAllLines(filePath).stream()
                    .flatMap(line -> List.of(line.split(",")).stream())
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .collect(Collectors.toSet()));
                this.listOfCacheFiles = new ArrayList<>(uniqueItems);
                
                // Initialize fileContentMap with keys from listOfCacheFiles
                for (Integer key : this.listOfCacheFiles) {
                    this.fileContentMap.put(key, new HashMap<>());
                }
            } catch (IOException e) {
                System.err.println("Error reading the cache file: " + e.getMessage());
            } catch (NumberFormatException e) {
                System.err.println("Error parsing an integer from the cache file: " + e.getMessage());
            }
        } else {
            System.err.println("Invalid file type. Must be a .cache file.");
        }

        this.scheduler = Executors.newScheduledThreadPool(1);
        this.scheduler.scheduleAtFixedRate(this::syncCacheContent, 0, this.updateCycleTimeSec, TimeUnit.SECONDS);

        // Add shutdown hook to shutdown the scheduler
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * Retrieves the cache content for a given query key.
     *
     * @param queryKey the key to look up in the cache
     * @return the HashMap associated with the query key, or null if the key is not found
     */
    public HashMap<String, String> getCacheContent(Integer queryKey) {
        this.objectIsBusy = true;
        int index = this.listOfCacheFiles.indexOf(queryKey);
        if (index == -1) {
            this.objectIsBusy = false;
            return null;
        }
        if (index > 0) {
            // Swap the queryKey with the element at index-1
            Integer temp = this.listOfCacheFiles.get(index - 1);
            this.listOfCacheFiles.set(index - 1, queryKey);
            this.listOfCacheFiles.set(index, temp);
        }
        this.objectIsBusy = false;
        return this.fileContentMap.get(queryKey);
    }

    /**
     * Updates the cache content for a given key.
     *
     * @param key     the key to update in the cache
     * @param content the new content to associate with the key
     */
    public void updateCacheContent(Integer key, HashMap<String, String> content) {
        this.objectIsBusy = true;
        int index = this.listOfCacheFiles.indexOf(key);
        if (index != -1) {
            // Key exists, update the existing HashMap with new entries
            this.fileContentMap.get(key).putAll(content);
        } else {
            // Key does not exist, insert it into the middle of the list
            int middleIndex = this.listOfCacheFiles.size() / 2;
            this.listOfCacheFiles.add(middleIndex, key);
            this.fileContentMap.put(key, content);

            // Check if the size exceeds the cache size
            if (this.listOfCacheFiles.size() > this.cacheSize) {
                Integer lastKey = this.listOfCacheFiles.remove(this.listOfCacheFiles.size() - 1);
                this.fileContentMap.remove(lastKey);
            }
        }
        this.objectIsBusy = false;
    }

    /**
     * Synchronizes the cache content by calling the readCacheFileFunction for each key
     * and updates the .cache file with the updated list of integer keys.
     */
    private void syncCacheContent() {
        if (this.objectIsBusy) {
            return;
        }
        this.objectIsBusy = true;
        System.out.println("Syncing cache content...");
        for (Integer key : this.listOfCacheFiles) {
            HashMap<String, String> updatedContent = this.readCacheFileFunction.apply(key);
            this.fileContentMap.put(key, updatedContent);
        }
        // Update the .cache file with the current list of cache keys
        try {
            String updatedKeys = this.listOfCacheFiles.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));
            Files.writeString(filePath, updatedKeys, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.err.println("Error updating the cache file: " + e.getMessage());
        }
        this.objectIsBusy = false;
    }

    /**
     * Shuts down the scheduler to stop periodic synchronization.
     */
    private void shutdown() {
        if (this.scheduler != null) {
            this.scheduler.shutdown();
            try {
                if (!this.scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    this.scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.scheduler.shutdownNow();
            }
        }
        System.out.println("Scheduler shut down.");
    }
}
