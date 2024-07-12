# SimpleKeyValueStorage

`SimpleKeyValueStorage` is a Java implementation of a simple key-value storage system that supports efficient reads, writes, and wildcard searches. It also includes optional parity file support for fault tolerance.

## Features

- **Simple Key-Value Storage**: Store and retrieve key-value pairs efficiently.
- **Wildcard Search**: Supports searching keys with wildcard characters.
- **Parity File Support**: Optional feature for fault tolerance using parity files.
- **Concurrency**: Optimized for concurrent read and write operations.

## Installation

1. **Clone the Repository**

   ```sh
   git clone https://github.com/your-username/SimpleKeyValueStorage.git
   cd SimpleKeyValueStorage
   ```

2. **Compile the Code**

   Ensure you have JDK installed. Then compile the code using:

   ```sh
   javac -d out src/application/SimpleKeyValueStorage.java src/application/Main.java
   ```

## Usage

### SimpleKeyValueStorage Class

The main class provides a simple API to store, retrieve, and manage key-value pairs.

#### Constructor

```java
public SimpleKeyValueStorage(String storageDirectory, int binCount, boolean enableParity, int parityGroupCount)
```

- `storageDirectory`: Path to the storage directory.
- `binCount`: Number of bins/shards in the key-value storage.
- `enableParity`: Enable parity feature for fault tolerance.
- `parityGroupCount`: Defines the maximum number of files ensured by a single parity file.

#### Methods

- **`void set(HashMap<String, String> inpHashMap)`**: Writes key-value pairs to the storage.
- **`HashMap<String, String> get(List<String> keyList)`**: Reads key-value pairs from the storage.
- **`void sync()`**: Saves the KVPool content to storage files.
- **`void remove(List<String> keyList)`**: Removes key-value pairs from the storage.

### Example

```java
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

public class ExampleUsage {
    public static void main(String[] args) {
        String storageDirectory = "exampleStorage";
        int binCount = 100;
        boolean enableParity = true;
        int parityGroupCount = 10;

        SimpleKeyValueStorage storage = new SimpleKeyValueStorage(storageDirectory, binCount, enableParity, parityGroupCount);

        // Setting key-value pairs
        HashMap<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        storage.set(data);
        storage.sync();

        // Getting values by keys
        List<String> keys = Arrays.asList("key1", "key2");
        try {
            HashMap<String, String> retrievedData = storage.get(keys);
            System.out.println(retrievedData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
```

## Performance Tips

- **Concurrency**: The class is optimized for concurrent read and write operations. Ensure you use it in a multi-threaded environment to leverage its full potential.
- **Bin Count**: Choose an appropriate bin count (`binCount`) based on your expected dataset size to balance the load across bins.
- **Parity Files**: Use the parity file feature (`enableParity`) if fault tolerance is required. This may slightly impact performance due to additional file operations.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by various key-value storage implementations.
