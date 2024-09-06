package data_processor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
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
public class SimpleCache {
    private static final String CACHE_EXTENSION = ".cache";
    private List<Integer> listOfCacheFiles;
    private HashMap<Integer, HashMap<String, String>> fileContentMap;
    private int cacheSize;
    private boolean objectIsBusy;
    private int updateCycleTimeSec;
    private Function<Integer, HashMap<String, String>> readCacheFileFunction;
    private ScheduledExecutorService scheduler;
    private Path filePath;
    private cacheMap simpleCacheMap;
    /**
     * Constructor initializes the SimpleCache with the given parameters.
     *
     * @param filePath             the path to the .cache file containing initial cache keys
     * @param objCacheSize         the maximum size of the cache
     * @param updateCycleTimeSec   the interval in seconds at which the cache content is synchronized
     * @param readCacheFileFunction a function that takes an integer key and returns a HashMap of cache content
     */
    SimpleCache(Path filePath, int objCacheSize, int updateCycleTimeSec, Function<Integer, HashMap<String, String>> readCacheFileFunction) {
        this(filePath, objCacheSize, updateCycleTimeSec);
        this.readCacheFileFunction = readCacheFileFunction;
    }

    /**
     * Constructor initializes the SimpleCache with the given parameters.
     * readCacheFileFunction is set to null.
     *
     * @param filePath             the path to the .cache file containing initial cache keys
     * @param objCacheSize         the maximum size of the cache
     * @param updateCycleTimeSec   the interval in seconds at which the cache content is synchronized
     */
    public SimpleCache(Path filePath, int objCacheSize, int updateCycleTimeSec) {
        this.listOfCacheFiles = new ArrayList<>();
        this.fileContentMap = new HashMap<>();
        this.cacheSize = objCacheSize;
        this.objectIsBusy = false;
        this.updateCycleTimeSec = updateCycleTimeSec;
        this.readCacheFileFunction = null;
        this.filePath = filePath;
        this.simpleCacheMap = new cacheMap();
        
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
            	List<Integer> uniqueItems = new ArrayList<>();
            	List<String> lines = Files.readAllLines(filePath);
                for (String line : lines) {
                    String[] parts = line.split(",");
                    for (String part : parts) {
                        String trimmed = part.trim();
                        if (!trimmed.isEmpty()) {
                            uniqueItems.add(Integer.parseInt(trimmed));
                        }
                    }
                }
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
     * Sets the readCacheFileFunction.
     *
     * @param readCacheFileFunction the function to set for reading cache file
     */
    public void setReadCacheFileFunction(Function<Integer, HashMap<String, String>> readCacheFileFunction) {
        this.readCacheFileFunction = readCacheFileFunction;
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
            if (this.readCacheFileFunction != null) {
                HashMap<String, String> updatedContent = this.readCacheFileFunction.apply(key);
                this.fileContentMap.put(key, updatedContent);
            }
        }
        // Update the .cache file with the current list of cache keys
        try {
            String updatedKeys = this.listOfCacheFiles.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));
            Files.write(filePath, Collections.singletonList(updatedKeys), StandardCharsets.UTF_8, 
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.err.println("Error updating the cache file: " + e.getMessage());
        }
        this.objectIsBusy = false;
    }

    /**
     * Shuts down the scheduler to stop periodic synchronization.
     */
    public void shutdown() {
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

    //Private class to lessen cache overhead during read operation
    private class cacheMap {
        private static List<String> keyList = new ArrayList<>();
        private static HashMap<String, String> cacheMapObj = new HashMap<>();
    
        public static List<String> updateQueryKeys(List<String> inpList) {
            List<String> result = new ArrayList<>();
            for (String key : inpList) {
                if (!keyList.contains(key)) {
                    result.add(key);
                }
            }
            return result;
        }
    
        public static void clearAll() {
            keyList.clear();
            cacheMapObj.clear();
        }
    
        public static HashMap<String, String> getfromCacheMap(List<String> inpList) {
            keyList.clear();
            HashMap<String, String> result = new HashMap<>();
            for (String key : inpList) {
                if (cacheMapObj.containsKey(key)) {
                    result.put(key, cacheMapObj.get(key));
                    keyList.add(key);
                }
            }
            return result;
        }
    
        public static void setCacheMapContent(HashMap<String, String> inpMap) {
            cacheMapObj.putAll(inpMap);
        }
    
        public static void removeCacheMapContent(List<String> inpList) {
            for (String key : inpList) {
                cacheMapObj.remove(key);
                keyList.remove(key);
            }
        }
    }
}
