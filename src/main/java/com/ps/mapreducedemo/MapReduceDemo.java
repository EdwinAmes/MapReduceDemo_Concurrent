package com.ps.mapreducedemo;

import com.ps.mapreducedemo.ingest.FileIngestor;
import com.ps.mapreducedemo.map.LineMapper;
import com.ps.mapreducedemo.reduce.WordReducer;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Edwin on 4/20/2016.
 */
public class MapReduceDemo {
    static Logger logger = LogManager.getLogger(MapReduceDemo.class);
    public static Object monitor = new Object();

    public static String INPUT_FOLDER = "input";
    public static String MAP_FOLDER = "mapped";
    public static String REDUCE_FOLDER = "reduced";

    public static void main(String[] args) {
        logger.trace("Map Reduce Begin");
        MapReduceDemo engine = new MapReduceDemo();
        //engine.doMapReduce(args[0]);
        engine.doMapReduce("C:\\Temp\\MapReduceDemo");
        logger.trace("Map Reduce Complete");
    }

    public Path getBasePath() {
        return basePath;
    }
    private Path basePath = null;

    // Threadsafe collections
    private Queue<Path> fileQueue = new ConcurrentLinkedQueue<Path>();
    private Queue<String> lineQueue = new ConcurrentLinkedQueue<String>();
    //   Partitions. For word-count demo using the words as the partitions.
    private Set<String> wordSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public final AtomicInteger fileRegisteredCount = new AtomicInteger(0);
    public final AtomicInteger fileIngestedCount = new AtomicInteger(0);
    public final AtomicBoolean fileIngestionComplete = new AtomicBoolean(false);

    /**
     * Check if all registered files have been processed.
     * Files are all registered before processing begins => no race condition.
     * TO DO: Will be responsible for NotifyAll to activate any waiting threads to finish processing
     * @return
     */
    public boolean isFileIngestionComplete()
    {
        if(!fileIngestionComplete.get())
        {
            boolean stateFlipped = fileIngestionComplete.compareAndSet(false,(getCountRemainingFiles() < 1));
            if(stateFlipped)
            {
                // Wakeup any threads waiting on lines
                synchronized (monitor)
                {
                    monitor.notifyAll();
                }
            }
        }
        return fileIngestionComplete.get();
    }

    /**
     * No need to synchronize.
     * Worst case is returns a larger count which causes another loop
     * @return
     */
    private int getCountRemainingFiles() {
        return fileRegisteredCount.get() - fileIngestedCount.get();
    }

    public final AtomicInteger lineProducedCount = new AtomicInteger(0);
    public final AtomicInteger lineConsumedCount = new AtomicInteger(0);


    private final int THREAD_POOL_COUNT = 10;
    private final int INGESTION_THREAD_COUNT = 1;

    private boolean cleanupOldResults() {
        Path mapPath = basePath.resolve(MapReduceDemo.MAP_FOLDER);
        try {
            FileUtils.cleanDirectory(mapPath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Map Folder {}",mapPath);
            return false;
        }

        Path reducePath = basePath.resolve(MapReduceDemo.REDUCE_FOLDER);
        try {
            FileUtils.cleanDirectory(reducePath.toFile());
        } catch (IOException e) {
            logger.error("Cannot Clean Reduce Folder {}",reducePath);
            return false;
        }
        return true;
    }

    private Path getBasePath(String rootPath){
        Path basePath = Paths.get(rootPath);
        try {
            basePath = basePath.toRealPath(LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            logger.error("Cannot Find Root Folder {}",rootPath);
            return null;
        }
        return basePath;
    }

    private void loadInputFilePathsIntoQueue() {
        List<Path> inputFileList = getSubPaths(basePath.resolve(MapReduceDemo.INPUT_FOLDER), "*.*");
        for(Path inputFile : inputFileList) {
            fileQueue.add(inputFile);
            fileRegisteredCount.incrementAndGet();
        }
    }

    /**
     *
     * @param rootPath
     */
    public void doMapReduce(String rootPath) {
        basePath = getBasePath(rootPath);
        if(basePath == null){
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_COUNT);

        logger.trace("Cleanup Old Results");
        if (!cleanupOldResults()) return;

        logger.trace("Begin Loading File List");
        // Find all files in /input directory and put paths in queue
        loadInputFilePathsIntoQueue();
        logger.trace("Done Loading File List");

        logger.trace("Begin File Ingestion and Mapping");

        List<Callable<Boolean>> ingestAndMapHandlers = new ArrayList<Callable<Boolean>>();
        // Using one thread pool for simplicity. Split threads between partitioning and mapping.
        // Assign threads to read files and produce lines => Ingest => Line Producer
        for(int threadIndex=0;threadIndex<INGESTION_THREAD_COUNT;threadIndex++) {
            ingestAndMapHandlers.add(new FileIngestor(fileQueue, lineQueue, this));
        }

        // Assign threads to read lines from queue and produce mapping files => Line Consumer
        for(int threadIndex=0;threadIndex<THREAD_POOL_COUNT-INGESTION_THREAD_COUNT;threadIndex++)
        {
            ingestAndMapHandlers.add(new LineMapper(lineQueue, wordSet, this));
        }

        boolean mappingSucceeded = false;
        try {
            executor.invokeAll(ingestAndMapHandlers);
            // Wait until all mapping complete => Barrier
            mappingSucceeded = true;
        } catch (InterruptedException e) {
            //
            logger.error("Error during ingestion and mapping", e);
        }

        if(mappingSucceeded) {
            logger.trace("Done File Ingestion and Mapping");

            logger.trace("Begin Result Reduction");

            // Load Set (needed for uniqueness constraint) into Queue (works better for multithreading)
            Queue<String> wordQueue = new ConcurrentLinkedQueue<String>();
            for(String word : wordSet)
            {
                wordQueue.add(word);
            }

            List<Callable<Boolean>> reduceHandlers = new ArrayList<Callable<Boolean>>();
            for(int threadIndex=0;threadIndex<THREAD_POOL_COUNT;threadIndex++)
            {
                reduceHandlers.add(new WordReducer(wordQueue, this));
            }
            try {
                executor.invokeAll(reduceHandlers);
            } catch (InterruptedException e) {
                logger.error("Error during reduction",e);
            }

            // Assign threads to read histogram files and generate final output => Reduce
            logger.trace("Done Result Reduction");
        }

        // Shutdown Executor
        try {
            logger.trace("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            logger.error("tasks interrupted", e);
        }
        finally {
            if (!executor.isTerminated()) {
                logger.error("cancel non-finished tasks");
            }
            executor.shutdownNow();
            logger.trace("shutdown finished");
        }
    }

    /**
     * Loads first level of child paths. Initially used to load a path for all files in a folder.
     * @param path
     */
    public List<Path> getSubPaths(Path path, String filter) {
        List<Path> subPathList = new ArrayList<Path>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, filter)) {
            for (Path file : stream) {
                subPathList.add(file);
            }
        } catch (IOException e) {
            logger.error("Error Loading Files in Path {}", path);
        }
        return subPathList;
    }
}
