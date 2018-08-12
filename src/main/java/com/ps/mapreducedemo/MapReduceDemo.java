package com.ps.mapreducedemo;

import com.ps.mapreducedemo.ingest.FileIngestor;
import com.ps.mapreducedemo.map.LineMapper;
import com.ps.mapreducedemo.reduce.WordReducer;
import com.ps.mapreducedemo.util.PathUtils;
import com.ps.mapreducedemo.util.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Edwin on 4/20/2016.
 */
public class MapReduceDemo {
    static Logger logger = LogManager.getLogger(MapReduceDemo.class);

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

    public Path loadBasePath() {
        return basePath;
    }
    private Path basePath = null;

    private final int THREAD_POOL_COUNT = 10;
    private final int INGESTION_THREAD_COUNT = 1;

    private final FileUtils fileUtils = new FileUtils();

    private boolean cleanupOldResults() {
        Path mapPath = basePath.resolve(MapReduceDemo.MAP_FOLDER);
        try {
            fileUtils.cleanDirectory(mapPath);
        } catch (IOException e) {
            logger.error("Cannot Clean Map Folder {}",mapPath);
            return false;
        }

        Path reducePath = basePath.resolve(MapReduceDemo.REDUCE_FOLDER);
        try {
            fileUtils.cleanDirectory(reducePath);
        } catch (IOException e) {
            logger.error("Cannot Clean Reduce Folder {}",reducePath);
            return false;
        }
        return true;
    }

    /**
     *
     * @param rootPath
     */
    public void doMapReduce(String rootPath) {
        basePath = PathUtils.loadBasePath(rootPath);
        if(basePath == null){
            return;
        }

        MapReduceState mapReduceState = new MapReduceState();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_COUNT);

        logger.trace("Cleanup Old Results");
        if (!cleanupOldResults()) return;

        logger.trace("Begin Loading File List");
        // Find all files in /input directory and put paths in queue
        PathUtils.loadInputFilePathsIntoQueue(basePath.resolve(MapReduceDemo.INPUT_FOLDER), mapReduceState);
        logger.trace("Done Loading File List");

        logger.trace("Begin File Ingestion and Mapping");

        List<Callable<Boolean>> ingestAndMapHandlers = new ArrayList<Callable<Boolean>>();
        // Using one thread pool for simplicity. Split threads between partitioning and mapping.
        // Assign threads to read files and produce lines => Ingest => Line Producer
        for(int threadIndex=0;threadIndex<INGESTION_THREAD_COUNT;threadIndex++) {
            ingestAndMapHandlers.add(new FileIngestor(mapReduceState, fileUtils));
        }

        // Assign threads to read lines from queue and produce mapping files => Line Consumer
        for(int threadIndex=0;threadIndex<THREAD_POOL_COUNT-INGESTION_THREAD_COUNT;threadIndex++)
        {
            ingestAndMapHandlers.add(new LineMapper(mapReduceState, this, fileUtils));
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

            List<Callable<Boolean>> reduceHandlers = new ArrayList<Callable<Boolean>>();
            Queue<String> wordQueue = mapReduceState.getWordQueueToProcess();
            for(int threadIndex=0;threadIndex<THREAD_POOL_COUNT;threadIndex++)
            {
                reduceHandlers.add(new WordReducer(wordQueue, this, fileUtils));
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
}
