package application;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class SimpleKeyValueStorage {
    // General Class Constants
    private static final String DIV_KEY = "~";
    public static final String WILDCARD_CHAR = "*";

    // Bin Files Class Constant
    private static final String FILE_PREFIX = "storageBin_";
    private static final String FILE_EXTENSION = ".dat";
    private static final String KEYWORD_KV = "KEYVAL";
    private static final String KEYWORD_TRIGRAM = "TRIGRM";
    private static final int NGRAM_VALUE = 8;

    // Parity Files Class Constant
    private static final String FILE_PREFIX_PARITY = "storageParity_";
    private static final String FILE_EXTENSION_PARITY = ".par";
    private static final String DIV_PARITY_FILE_NAME = "_";
    private static final int MAX_RECOVERY_COUNT = 5; // Maximum Recovery attempts before Fully stopping recovery efforts

    // Object Variables
    private Path keyValueStorageLocation; // Stores Location of the Storage Directory
    private int storageBinCount; // Contains number of data files in the Storage Directory
    private ConcurrentHashMap<String, String> KVPool; // Storage of Key-Value Pairs before synchronization
    private boolean enableParityFeature; // Enable parity feature for this class
    private int storageParityGroupSize; // Defines the maximum number of files ensured by a single parity file

    // Executor for parallel tasks
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    // Locks for synchronizing file access
    private final ConcurrentHashMap<Integer, Object> fileLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> parityFileLocks = new ConcurrentHashMap<>();

    // Description: Class constructor
    /* Input Parameters
     *      > storageDirectory       (String)        - Contains link to key-value storage directory
     *      > binCount               (Integer)       - number of bins/shards in the key-value storage
     * Output: SimpleKeyValueStorage Object
     */
    public SimpleKeyValueStorage(String storageDirectory, int binCount, boolean enableParity, int parityGroupCount) {
        keyValueStorageLocation = Paths.get(storageDirectory);
        storageBinCount = binCount;
        enableParityFeature = enableParity;
        storageParityGroupSize = parityGroupCount;
        KVPool = new ConcurrentHashMap<>();
        createStorageDirectoryIfNotExists();
    }

    private void createStorageDirectoryIfNotExists() {
        if (!(Files.exists(keyValueStorageLocation) && Files.isDirectory(keyValueStorageLocation))) {
            // Create the missing Directory
            try {
                Files.createDirectories(keyValueStorageLocation);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

 // Description: Imports all key-value pairs from a source storage into this storage instance
    /*
     * Input Parameters
     *      > sourceStorage         (SimpleKeyValueStorage)     - The source storage instance from which data will be imported
     *      > generateNGRAM         (boolean)                   - Flag to indicate whether to generate NGRAMs during the set operation
     * Output: None
     * Throws: IOException if there is an error reading from the source or writing to this instance
     */
    public void transferFrom(SimpleKeyValueStorage sourceStorage) throws IOException {
    	transferFrom(sourceStorage,false);
    }
    
    public void transferFrom(SimpleKeyValueStorage sourceStorage, boolean generateNGRAM) throws IOException {
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < sourceStorage.storageBinCount; i++) {
            final int binIndex = i;
            futures.add(executorService.submit(() -> {
                Path binFilePath = sourceStorage.keyValueStorageLocation.resolve(SimpleKeyValueStorage.FILE_PREFIX + binIndex + SimpleKeyValueStorage.FILE_EXTENSION);
                if (Files.exists(binFilePath)) {
                    HashMap<String, String> sourceFileContent = sourceStorage.readFileContents(binIndex);
                    HashMap<String, String> transferContent = new HashMap<>();
                    sourceFileContent.forEach((key, value) -> {
                        if (key.contains(SimpleKeyValueStorage.KEYWORD_KV)) {
                            String strippedKey = key.replace(SimpleKeyValueStorage.KEYWORD_KV + SimpleKeyValueStorage.DIV_KEY, "");
                            transferContent.put(strippedKey, value);
                        }
                    });
                    set(transferContent, generateNGRAM);
                }
                return null;
            }));
        }
        waitForCompletion(futures);
        sync();
    }


    // Description: Reads Key-Value Pairs from Storage
    /*
     * Input Parameters
     *      > keyList               (List<String>)       - Contains list of keys to be searched. Keys with wildcard characters are allowed
     * Output: HashMap containing keys with matching values only
     */
    public HashMap<String, String> get(List<String> keyList) {
        HashMap<String, String> outputHashMap = new HashMap<>();
        List<String> keysWithWildcard = keyList.stream().filter(s -> s.contains(WILDCARD_CHAR)).collect(Collectors.toList());
        HashMap<String, List<String>> matchingKeysPerWildCardKey = getKeysFromWildCardList(keysWithWildcard);
        List<String> keysWithoutWildcard = keyList.stream().filter(s -> !s.contains(WILDCARD_CHAR)).collect(Collectors.toList());
        List<String> queryKeyList = new ArrayList<>(keysWithoutWildcard);
        matchingKeysPerWildCardKey.values().forEach(queryKeyList::addAll);

        List<String> tmp_queryKeyList = new ArrayList<>(queryKeyList);
        for (String x : tmp_queryKeyList) {
            String tmp_KVPoolValue = KVPool.get(KEYWORD_KV + DIV_KEY + x);
            if (tmp_KVPoolValue != null) {
                outputHashMap.put(x, tmp_KVPoolValue);
                queryKeyList.remove(x);
            }
        }

        List<String> modQueryKeyList = queryKeyList.stream().map(s -> KEYWORD_KV + DIV_KEY + s).collect(Collectors.toList());
        List<Integer> tmp_binFileList = getListOfBinFiles(modQueryKeyList, false);

        List<Future<HashMap<String, String>>> futures = new ArrayList<>();
        for (int y : tmp_binFileList) {
            futures.add(executorService.submit(() -> readFileContents(y)));
        }
        HashMap<String,String> binContents = new HashMap<>();
        try {
            for (Future<HashMap<String, String>> future : futures) {
                binContents.putAll(future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        for (String x : queryKeyList) {
            outputHashMap.put(x, binContents.get(KEYWORD_KV + DIV_KEY + x));
        }

        return outputHashMap;
    }

    // Description: Writes Key-Value Pairs to KVPool
    /*
     * Input Parameters
     *      > keyValuePair          (HashMap<String,String>)        - Contains list of key-value pairs to be stored. Keys with wildcard characters are allowed
     * Output: None
     */
    public void set(HashMap<String, String> inpHashMap) {
        set(inpHashMap, true);
    }

    public void set(HashMap<String, String> inpHashMap, boolean generateNGRAM) {
        HashMap<String, String> tmp_kvPoolMap = new HashMap<>();
        List<String> keysWithWildcard = inpHashMap.keySet().stream().filter(s -> s.contains(WILDCARD_CHAR)).collect(Collectors.toList());
        HashMap<String, List<String>> matchingKeysPerWildCardKey = getKeysFromWildCardList(keysWithWildcard);
        List<String> keysWithoutWildcard = inpHashMap.keySet().stream().filter(s -> !s.contains(WILDCARD_CHAR)).collect(Collectors.toList());

        for (String x : matchingKeysPerWildCardKey.keySet()) {
            for (String y : matchingKeysPerWildCardKey.get(x)) {
                tmp_kvPoolMap.put(KEYWORD_KV + DIV_KEY + y, inpHashMap.get(x));
            }
        }

        for (String x : keysWithoutWildcard) {
            tmp_kvPoolMap.put(KEYWORD_KV + DIV_KEY + x, inpHashMap.get(x));
        }
        KVPool.putAll(tmp_kvPoolMap);

        if (generateNGRAM) {
            updateNGramUsingKeys(keysWithoutWildcard);
        }
    }

    // Description: Saves KVPool Content to Storage Files
    public void sync() {
        HashMap<Integer, HashMap<String, String>> fileUpdateList = new HashMap<>();
        for (String x : KVPool.keySet()) {
            int binFile = hashKey(x);
            fileUpdateList.computeIfAbsent(binFile, k -> new HashMap<>()).put(x, KVPool.get(x));
        }

        List<Future<Void>> futures = new ArrayList<>();
        for (Map.Entry<Integer, HashMap<String, String>> entry : fileUpdateList.entrySet()) {
            int binFile = entry.getKey();
            HashMap<String, String> dataMap = entry.getValue();
            futures.add(executorService.submit(() -> {
                synchronized (getFileLock(binFile)) {
                    try {
                        HashMap<String, String> binFileContent = readFileContents(binFile);
                        binFileContent.putAll(dataMap);
                        writeFileContents(binFile, binFileContent);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return null;
            }));
        }

        waitForCompletion(futures);
        KVPool.clear();
    }

    // Description: Removes Key-Value Pairs from KVPool and storage
    /*
     * Input Parameters
     *      > keyList               (List<String>)       - Contains list of keys to be removed. Keys with wildcard characters are allowed
     * Output: None
     */
    public void remove(List<String> keyList) {
        List<String> keysWithWildcard = keyList.stream().filter(s -> s.contains(WILDCARD_CHAR)).collect(Collectors.toList());
        HashMap<String, List<String>> matchingKeysPerWildCardKey = getKeysFromWildCardList(keysWithWildcard);
        List<String> keysWithoutWildcard = keyList.stream().filter(s -> !s.contains(WILDCARD_CHAR)).collect(Collectors.toList());
        List<String> keysToDelete = new ArrayList<>(keysWithoutWildcard);
        matchingKeysPerWildCardKey.values().forEach(keysToDelete::addAll);
        keysToDelete.forEach(key -> KVPool.remove(KEYWORD_KV + DIV_KEY + key));
        Map<Integer, Set<String>> fileUpdates = groupUpdatesByFile(keysToDelete);

        List<Future<Void>> futures = new ArrayList<>();
        for (Map.Entry<Integer, Set<String>> entry : fileUpdates.entrySet()) {
            int binNumber = entry.getKey();
            Set<String> keysInFile = entry.getValue();
            futures.add(executorService.submit(() -> {
                synchronized (getFileLock(binNumber)) {
                    try {
                        HashMap<String, String> fileContent = readFileContents(binNumber);
                        boolean fileChanged = false;
                        for (String key : keysInFile) {
                            if (key.startsWith(KEYWORD_KV + DIV_KEY)) {
                                fileContent.remove(key);
                                fileChanged = true;
                            } else if (key.startsWith(KEYWORD_TRIGRAM + DIV_KEY)) {
                                String value = fileContent.get(key);
                                if (value != null) {
                                    List<String> keys = new ArrayList<>(Arrays.asList(value.split(",")));
                                    keys.removeAll(keysToDelete);
                                    if (keys.isEmpty()) {
                                        fileContent.remove(key);
                                    } else {
                                        fileContent.put(key, String.join(",", keys));
                                    }
                                    fileChanged = true;
                                }
                            }
                        }
                        if (fileChanged) {
                            writeFileContents(binNumber, fileContent);
                        }
                    } catch (IOException e) {
                        System.err.println("<SimpleKeyValueStorage> Error updating file: " + e.getMessage());
                    }
                }
                return null;
            }));
        }

        waitForCompletion(futures);
    }

    private Map<Integer, Set<String>> groupUpdatesByFile(List<String> keysToDelete) {
        Map<Integer, Set<String>> fileUpdates = new HashMap<>();

        for (String key : keysToDelete) {
            String kvKey = KEYWORD_KV + DIV_KEY + key;
            int kvBinFile = hashKey(kvKey);
            fileUpdates.computeIfAbsent(kvBinFile, k -> new HashSet<>()).add(kvKey);

            List<String> nGrams = generateNGrams(key, NGRAM_VALUE, WILDCARD_CHAR);
            for (String nGram : nGrams) {
                String trigramKey = KEYWORD_TRIGRAM + DIV_KEY + nGram;
                int nGramBinFile = hashKey(trigramKey);
                fileUpdates.computeIfAbsent(nGramBinFile, k -> new HashSet<>()).add(trigramKey);
            }
        }

        return fileUpdates;
    }

    // FILE READ/WRITE OPERATION----------------------------------------------------------------------
    /**
     * Description: Reads the contents of a specified bin file based on its number. This method constructs the file path,
     * checks its existence, and reads its lines. It verifies the integrity of the file contents using a CRC32 checksum comparison.
     * If the checksums match, the data is parsed into a key-value map; otherwise, it reports an error. The method is prepared to handle
     * possible exceptions such as an empty file, checksum mismatch, or format errors.
     *
     * Input Parameters:
     *   > binNumber    (int)   - The number of the bin file to read, used to construct the file path.
     *
     * Output: Returns a HashMap where each line from the binary file, if validated by checksum, is parsed into a key-value pair.
     * If the file does not exist, is empty, or if there is a checksum mismatch, appropriate exceptions are thrown or errors are logged.
     */
    protected HashMap<String, String> readFileContents(int binNumber) throws IOException {
        try {
            return readFileContents(binNumber, 0);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    protected HashMap<String, String> readFileContents(int binNumber, int loopcount) throws IOException {
        Path filePath = keyValueStorageLocation.resolve(FILE_PREFIX + binNumber + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            return new HashMap<>();
        }

        HashMap<String, String> outputMap = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            List<String> allLines = Files.readAllLines(filePath);
            if (allLines.isEmpty()) {
                throw new IOException("File is empty");
            }

            long fileChecksum = Long.parseLong(allLines.get(0));
            List<String> dataLines = allLines.stream()
            	    .skip(1)
            	    .filter(line -> !line.trim().isEmpty() && !line.trim().startsWith("//"))
            	    .collect(Collectors.toList());
            if (enableParityFeature) {
                CRC32 crc = new CRC32();
                dataLines.forEach(line -> {
                    crc.update(line.getBytes());
                    crc.update(System.lineSeparator().getBytes());
                });
                long computedChecksum = crc.getValue();

                if (fileChecksum != computedChecksum) {
                    throw new IOException("Checksum mismatch detected");
                }
            }
            dataLines.forEach(line -> parseFormattedLine(line, outputMap));
        } catch (NumberFormatException | IOException e) {
            System.err.println("<SimpleKeyValueStorage> Error reading the file: " + e.getMessage());
            if (enableParityFeature) {
                if (loopcount < MAX_RECOVERY_COUNT) {
                    recoverBinFile(binNumber);
                    return readFileContents(binNumber, loopcount + 1);
                } else {
                    throw new IOException(e.getMessage());
                }
            }
        }
        return outputMap;
    }

    /**
     * Description: Writes the given data map to a specified binary file, computing and including a CRC32 checksum for data integrity verification.
     * The method ensures that the necessary directories and the file exist before writing. It concatenates all key-value pairs into a single string,
     * calculates its checksum, and writes both the checksum and the data to the file. Errors in file operations are handled and logged.
     *
     * Input Parameters:
     *   > binNumber    (int)                - The number of the binary file where the data is to be written.
     *   > dataMap      (Map<String, String>) - Map containing the data to be written to the file, where each key-value pair represents one line.
     *
     * Output: No return value. The data, along with a checksum, is written to the specified file. Errors during the process are logged, and potential
     * recovery actions are noted as TODOs in the catch blocks.
     */
    protected void writeFileContents(int binNumber, Map<String, String> dataMap) throws IOException {
        try {
            writeFileContents(binNumber, dataMap, 0);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    protected void writeFileContents(int binNumber, Map<String, String> dataMap, int loopcount) throws IOException {
        Path filePath = keyValueStorageLocation.resolve(FILE_PREFIX + binNumber + FILE_EXTENSION);
        // Ensure that parent directories exist and create file if it does not exist
        try {
            if (Files.notExists(filePath)) {
                Files.createDirectories(filePath.getParent()); // Ensure the directory exists
                Files.createFile(filePath); // Explicitly create the file if it does not exist
            }
            // Use a StringBuilder to gather all the data for checksum calculation
            StringBuilder dataBuilder = new StringBuilder();
            TreeMap<String, String> sortedDataMap = new TreeMap<>(dataMap);
            for (Map.Entry<String, String> entry : sortedDataMap.entrySet()) {
                String formattedLine = createFormattedLine(entry.getKey(), entry.getValue());
                dataBuilder.append(formattedLine).append(System.lineSeparator());
            }

            // Calculate checksum
            CRC32 crc = new CRC32();
            crc.update(dataBuilder.toString().getBytes());
            long checksum = crc.getValue();

            // Write checksum and data to file
            synchronized (getFileLock(binNumber)) {
                try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                    writer.write(String.valueOf(checksum)); // Write checksum in the first line
                    writer.newLine();
                    writer.write(dataBuilder.toString()); // Write data
                }
            }

            // Update Parity File
            if (enableParityFeature)
                updateParityFile(binNumber);
        } catch (IOException e) {
            System.err.println("<SimpleKeyValueStorage> Error writing to the file: " + e.getMessage());
            if (enableParityFeature) {
                if (loopcount < MAX_RECOVERY_COUNT) {
                    // Any Error that occurs during the write process will trigger a bin file recovery
                    recoverBinFile(binNumber);
                    writeFileContents(binNumber, dataMap, loopcount + 1);
                } else
                    throw new IOException(e.getMessage());
            }
        }
    }

    // Description: Updates the Parity file related to a bin file
    private void updateParityFile(int binNumber) throws IOException {
        List<Path> listOfBinFilesInParityGroup = new ArrayList<>();

        // Identify bin files in the Parity group where bin file number binNumber belongs
        int modValueBinNumber = binNumber % storageParityGroupSize;
        int parityBinNumberStart = binNumber - modValueBinNumber;
        int parityBinNumberEnd = binNumber + (storageParityGroupSize - modValueBinNumber - 1);

        // Add main bin File in the Path list
        for (int i = parityBinNumberStart; i <= parityBinNumberEnd; i++)
            if (Files.exists(keyValueStorageLocation.resolve(FILE_PREFIX + i + FILE_EXTENSION)))
                listOfBinFilesInParityGroup.add(keyValueStorageLocation.resolve(FILE_PREFIX + i + FILE_EXTENSION));

        // Build target fileName
        Path targetParityFile = keyValueStorageLocation.resolve(FILE_PREFIX_PARITY + parityBinNumberStart + DIV_PARITY_FILE_NAME + parityBinNumberEnd + FILE_EXTENSION_PARITY);

        // Build the ParityFile
        synchronized (getParityFileLock(parityBinNumberStart, parityBinNumberEnd)) {
            xorFiles(listOfBinFilesInParityGroup, targetParityFile);
        }
    }

    // Description: Recovers a Bin File
    private void recoverBinFile(int binNumber) throws IOException {
        List<Path> listOfBinFilesInParityGroupPlusParity = new ArrayList<>();

        // Identify bin files in the Parity group where bin file number binNumber belongs
        int modValueBinNumber = binNumber % storageParityGroupSize;
        int parityBinNumberStart = binNumber - modValueBinNumber;
        int parityBinNumberEnd = binNumber + (storageParityGroupSize - modValueBinNumber - 1);

        // Add Bin Files in the Path list + Parity File
        for (int i = parityBinNumberStart; i <= parityBinNumberEnd; i++)
            if (i != binNumber && Files.exists(keyValueStorageLocation.resolve(FILE_PREFIX + i + FILE_EXTENSION)))
                listOfBinFilesInParityGroupPlusParity.add(keyValueStorageLocation.resolve(FILE_PREFIX + i + FILE_EXTENSION));
        listOfBinFilesInParityGroupPlusParity.add(keyValueStorageLocation.resolve(FILE_PREFIX_PARITY + parityBinNumberStart + DIV_PARITY_FILE_NAME + parityBinNumberEnd + FILE_EXTENSION_PARITY));

        // Build target fileName
        Path targetRecoveredFile = keyValueStorageLocation.resolve(FILE_PREFIX + binNumber + FILE_EXTENSION);

        // Build the ParityFile
        synchronized (getParityFileLock(parityBinNumberStart, parityBinNumberEnd)) {
            xorFiles(listOfBinFilesInParityGroupPlusParity, targetRecoveredFile);
        }
    }

    // MISC FUNCTION----------------------------------------------------------------------------------
    // Description: Converts a Comma-Separated String into a List<String Object>
    private List<String> commaSeparatedToList(String csvString) {
        if (csvString == null || csvString.isEmpty()) {
            return Arrays.asList(); // Return an empty list if the input string is null or empty
        }
        return Arrays.stream(csvString.split(","))
                .map(String::trim) // Trim each element
                .collect(Collectors.toList()); // Collect results into a list
    }

    // Description: creates the standard line used in the key-value storage files
    private String createFormattedLine(String key, String value) {
        return "\"" + key + "\":\"" + value + "\";";
    }

    // Description: Generate an n-gram list
    private List<String> generateNGrams(String inpString, int nGramValue, String limiter) {
        List<String> nGrams = new ArrayList<>();

        // Ensure the nGramValue is valid and the string length is adequate
        if (nGramValue > 0 && inpString.length() >= nGramValue) {
            // Check if the limiter is non-null and non-empty
            boolean limiterPresent = (limiter != null && !limiter.isEmpty());
            for (int i = 0; i <= inpString.length() - nGramValue; i++) {
                String subStr = inpString.substring(i, i + nGramValue);
                // Add the substring if limiter is not present or substring does not contain limiter
                if ((!limiterPresent || !subStr.contains(limiter)) && !nGrams.contains(subStr))
                    nGrams.add(subStr);
            }
        }
        return nGrams;
    }

    // Description: Gets list of Bin Files from list of keys
    /*
     * Input Parameters
     *      > listOfString              (List<String>)       - Contains list of keys to be searched hashed
     *      > doesListContainTrigrams   (boolean)            - Prefix Selection parameter
     * Output: HashMap containing keys with matching values only
     */
    private List<Integer> getListOfBinFiles(List<String> listOfString, boolean doesListContainTrigrams) {
        List<Integer> outputList = new ArrayList<>();
        for (String x : listOfString) {
            int tmpBinNumber = hashKey(x);
            if (!outputList.contains(tmpBinNumber))
                outputList.add(tmpBinNumber);
        }
        return outputList;
    }

    /**
     * Description: Retrieves keys corresponding to wildcard entries by generating n-grams and searching both
     * a key-value pool (KVPool) and storage files. This method processes each wildcard entry to create n-grams,
     * searches these n-grams in the KVPool, and if not found, proceeds to check storage files. The method ensures
     * that only matching keys present in all n-gram entries of a wildcard are retained.
     *
     * Input Parameters:
     *   > listOfWildcardKeys   (List<String>)  - List of wildcard keys for which corresponding real keys are to be searched.
     *
     * Output: A HashMap where each wildcard key is mapped to a list of matching keys found in the KVPool or storage files.
     * The method returns this map. If no matches are found for a wildcard, an empty list is associated with it.
     */
    private HashMap<String, List<String>> getKeysFromWildCardList(List<String> listOfWildcardKeys) {
        HashMap<String, List<String>> wildCardNGramKeys = new HashMap<>();
        for (String x : listOfWildcardKeys) {
            List<String> tmp_nGramList = generateNGrams(x, NGRAM_VALUE, WILDCARD_CHAR).stream()
                    .map(s -> KEYWORD_TRIGRAM + DIV_KEY + s)
                    .collect(Collectors.toList());
            HashMap<String, String> compiledBinFileContent = new HashMap<>();

            List<String> tmp_nGramList1 = new ArrayList<>(tmp_nGramList);
            for (String z : tmp_nGramList1) {
                String tmp_KVPoolValue = KVPool.get(z);
                if (tmp_KVPoolValue != null) {
                    compiledBinFileContent.put(z, tmp_KVPoolValue);
                    tmp_nGramList.remove(z);
                }
            }

            List<Integer> tmp_binFileList = getListOfBinFiles(tmp_nGramList, true);
            List<Future<HashMap<String, String>>> futures = new ArrayList<>();
            for (int y : tmp_binFileList) {
                futures.add(executorService.submit(() -> readFileContents(y)));
            }

            try {
                for (Future<HashMap<String, String>> future : futures) {
                    compiledBinFileContent.putAll(future.get());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            boolean FIRST_PASS = true;
            List<String> matchingKeys = new ArrayList<>();
            for (String q : tmp_nGramList) {
                String nGramMatchingKeysCommaSeparated = compiledBinFileContent.get(q);
                if (nGramMatchingKeysCommaSeparated != null) {
                    List<String> tmp_matchingKeys = commaSeparatedToList(nGramMatchingKeysCommaSeparated);
                    if (FIRST_PASS) {
                        FIRST_PASS = false;
                        matchingKeys = new ArrayList<>(tmp_matchingKeys);
                    } else
                        matchingKeys.retainAll(tmp_matchingKeys);

                    if (matchingKeys.size() <= 1)
                        break;
                }
            }

            wildCardNGramKeys.put(x, matchingKeys);
        }

        return wildCardNGramKeys;
    }

    /**
     * Description: Updates n-grams using a list of keys by interacting with a key-value pool and storage files.
     * It first generates n-grams for each key, then checks and updates existing keys in the KVPool. If an n-gram
     * does not exist in the KVPool, it searches in storage files or treats it as new. Updates to the KVPool are made accordingly.
     *
     * Input Parameters:
     *   > listOfKeys   (List<String>)  - List of keys to generate and update n-grams.
     *
     * Output: N-grams are updated in the KVPool with associated keys. If an n-gram is not present, it's either fetched from storage files
     * or initialized with the current key. No return value; side effects include updating the KVPool and potentially reading from storage.
     */

    private void updateNGramUsingKeys(List<String> listOfKeys) {
        for (String x : listOfKeys) {
            List<String> listOfnGram = generateNGrams(x, NGRAM_VALUE, WILDCARD_CHAR).stream()
                    .map(s -> KEYWORD_TRIGRAM + DIV_KEY + s)
                    .collect(Collectors.toList());
            List<String> copyOfListOfnGram = new ArrayList<>(listOfnGram);

            for (String y : copyOfListOfnGram) {
                String tmpKVPoolVal = KVPool.get(y);
                if (tmpKVPoolVal != null) {
                    List<String> tmpString = Arrays.asList(tmpKVPoolVal.split("\\s*,\\s*"));
                    if (!tmpString.contains(x)) {
                        tmpKVPoolVal = tmpKVPoolVal + "," + x;
                        KVPool.put(y, tmpKVPoolVal);
                        listOfnGram.remove(y);
                    }
                }
            }

            HashMap<String, String> compiledBinFileContent = new HashMap<>();
            List<Integer> tmp_binFileList = getListOfBinFiles(listOfnGram, true);
            List<Future<HashMap<String, String>>> futures = new ArrayList<>();
            for (int y : tmp_binFileList) {
                futures.add(executorService.submit(() -> readFileContents(y)));
            }

            try {
                for (Future<HashMap<String, String>> future : futures) {
                    compiledBinFileContent.putAll(future.get());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            for (String y : listOfnGram) {
                String tmpBinContent = compiledBinFileContent.get(y);
                if (tmpBinContent != null) {
                    List<String> tmpString = Arrays.asList(tmpBinContent.split("\\s*,\\s*"));
                    if (!tmpString.contains(x)) {
                        tmpBinContent = tmpBinContent + "," + x;
                        KVPool.put(y, tmpBinContent);
                    }
                } else {
                    tmpBinContent = x;
                    KVPool.put(y, tmpBinContent);
                }
            }
        }
    }

    // Description: Simple Hash Function Implementation
    /* Input Parameters
     *      > key                (String)        - String to be hashed by the function
     * Output: integer hash number assigned to the string. Value of integer is between 0 to storageBinCount-1
     */
    private int hashKey(String key) {
        int hash = 0;
        for (int i = 0; i < key.length(); i++) {
            int keyCharacter = (int) key.charAt(i);
            hash = (hash << 5) - hash + keyCharacter;
            hash = hash & hash; // Convert to 32bit integer
        }
        return Math.abs(hash) % storageBinCount;
    }

    // Description: parses the standard line used in the key-value storage files
    private void parseFormattedLine(String line, Map<String, String> outputMap) {
        String relevantPart = line.split(";")[0].trim();
        String[] parts = relevantPart.split("\":\"");
        if (parts.length == 2) {
            String key = parts[0].trim().replace("\"", "");
            String value = parts[1].trim().replace("\"", "");
            outputMap.put(key, value);
        }
    }

    /**
     * Description: Performs a byte-wise XOR operation on multiple files and writes the output to a target file.
     * This method handles files of different sizes by treating absent bytes in shorter files as zeros.
     *
     * Input Parameters:
     *   > inpFiles    (List<Path>)    - List of paths for the files to XOR.
     *   > targetFile  (Path)          - Path for the output file where the XOR result is saved.
     *
     * Output: The result is directly written to the targetFile. Errors during file processing are logged.
     */
    public void xorFiles(List<Path> inpFiles, Path targetFile) throws IOException {
        FileInputStream[] streams = new FileInputStream[inpFiles.size()];
        try {
            long maxLen = 0;
            for (Path file : inpFiles) {
                maxLen = Math.max(maxLen, Files.size(file));
            }

            for (int i = 0; i < inpFiles.size(); i++) {
                streams[i] = new FileInputStream(inpFiles.get(i).toFile());
            }

            if (Files.exists(targetFile)) {
                Files.delete(targetFile);
            }

            try (FileOutputStream out = new FileOutputStream(targetFile.toFile())) {
                byte[] buffer = new byte[4096];
                byte[][] fileBuffers = new byte[inpFiles.size()][4096];

                for (long pos = 0; pos < maxLen; pos += 4096) {
                    Arrays.fill(buffer, (byte) 0);
                    int bytesRead = 0;

                    for (int i = 0; i < inpFiles.size(); i++) {
                        Arrays.fill(fileBuffers[i], (byte) 0);
                        int read = streams[i].read(fileBuffers[i]);
                        if (read > bytesRead) {
                            bytesRead = read;
                        }

                        for (int j = 0; j < bytesRead; j++) {
                            buffer[j] ^= fileBuffers[i][j];
                        }
                    }

                    out.write(buffer, 0, bytesRead);
                }
            }
        } catch (IOException e) {
            System.err.println("Error processing files: " + e.getMessage());
            throw e;
        } finally {
            for (FileInputStream stream : streams) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        System.err.println("Error closing input stream: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void waitForCompletion(List<Future<Void>> futures) {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private Object getFileLock(int binFile) {
        return fileLocks.computeIfAbsent(binFile, k -> new Object());
    }

    private Object getParityFileLock(int parityStart, int parityEnd) {
        String parityFileKey = parityStart + "_" + parityEnd;
        return parityFileLocks.computeIfAbsent(parityFileKey, k -> new Object());
    }
}
