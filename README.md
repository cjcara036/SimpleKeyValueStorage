# Simple Key-Value Storage with Caching

This project implements a simple key-value storage system with caching capabilities in Java. It consists of three main classes: SimpleKeyValueStorage, SimpleCache, and a Main class for benchmarking.

## Overview

### SimpleKeyValueStorage

SimpleKeyValueStorage is a class that provides a key-value storage system with the following features:
- Sharded storage across multiple files
- Support for wildcard searches
- Optional parity file generation for data recovery
- N-gram indexing for efficient searching

### SimpleCache

SimpleCache is a caching mechanism designed to work alongside SimpleKeyValueStorage. It provides:
- In-memory caching of frequently accessed data
- Periodic synchronization with the underlying storage
- Configurable cache size and update cycle

### Main

The Main class contains a benchmark test to evaluate the performance of the SimpleKeyValueStorage system with caching.

## How to Use

### SimpleKeyValueStorage

To use SimpleKeyValueStorage, follow these steps:

1. Create an instance of SimpleKeyValueStorage:

```java
String storageDirectory = "path/to/storage";
int binCount = 100;
boolean enableParity = true;
int parityGroupCount = 10;
SimpleCache cache = new SimpleCache(Paths.get("cache.cache"), 1024, 120);

SimpleKeyValueStorage storage = new SimpleKeyValueStorage(storageDirectory, binCount, enableParity, parityGroupCount, cache);
```

2. Set key-value pairs:

```java
HashMap<String, String> data = new HashMap<>();
data.put("key1", "value1");
data.put("key2", "value2");
storage.set(data);
storage.sync(); // Write to storage
```

3. Get values:

```java
List<String> keys = Arrays.asList("key1", "key2");
HashMap<String, String> result = storage.get(keys);
```

4. Remove keys:

```java
List<String> keysToRemove = Arrays.asList("key1", "key2");
storage.remove(keysToRemove);
```

### SimpleCache

SimpleCache is typically used in conjunction with SimpleKeyValueStorage. To use it:

1. Create an instance of SimpleCache:

```java
Path cacheFilePath = Paths.get("cache.cache");
int cacheSize = 1024;
int updateCycleSeconds = 120;
SimpleCache cache = new SimpleCache(cacheFilePath, cacheSize, updateCycleSeconds);
```

2. Set the read cache file function:

```java
cache.setReadCacheFileFunction(key -> {
    try {
        return storage.readFileContents(key, 0, true);
    } catch (IOException e) {
        e.printStackTrace();
    }
    return null;
});
```

3. The cache will automatically be used by SimpleKeyValueStorage when provided in its constructor.

## Setting Up the Benchmark Test

To run the benchmark test in Main.java:

1. Ensure you have the necessary storage directory set up.
2. Run the `main` method in the `Main` class.

The benchmark test will:
1. Initialize SimpleKeyValueStorage and SimpleCache
2. Generate test data (10,000 entries by default)
3. Perform write operations and measure write rate
4. Perform read operations and measure read rate
5. Perform wildcard search operations and measure search rate

You can modify the following parameters in `Main.java` to adjust the benchmark:

```java
String storageDirectory = "benchmarkStorage";
String cacheFile = "benchmarkCache.cache";
int binCount = 100;
boolean enableParity = true;
int parityGroupCount = 10;
int numberOfEntries = 10000;
int numberOfReads = 100;
int numberOfWildcardSearches = 100;
```

After running the benchmark, the results will be printed to the console, showing the duration and rate for write, read, and wildcard search operations.

Note: Remember to call `cache.shutdown()` when you're done using the system to ensure proper cleanup.
```

This README.md provides an overview of the classes, instructions on how to use SimpleKeyValueStorage and SimpleCache, and explains how to set up and run the benchmark test in Main.java. You can further customize this README as needed for your project.
