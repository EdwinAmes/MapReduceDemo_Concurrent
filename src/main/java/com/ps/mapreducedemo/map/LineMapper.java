package com.ps.mapreducedemo.map;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.util.LineHistogramMaker;
import com.ps.mapreducedemo.MapReduceNodeProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Created by Edwin on 4/21/2016.
 * Creates histogram (word frequency count) for a line of text
 * Writes a file named word.thread.id.cnt for each word found
 */
public class LineMapper extends MapReduceNodeProcessor {
    static Logger logger = LogManager.getLogger(LineMapper.class);

    private Queue<String> lineQueue;
    private Set<String> wordSet;
    private MapReduceDemo context;
    private LineHistogramMaker histogramMaker = new LineHistogramMaker();

    public LineMapper(Queue<String> lineQueue, Set<String> wordQueue, MapReduceDemo context) {
        this.lineQueue = lineQueue;
        this.wordSet = wordQueue;
        this.context = context;
    }


    @Override
    public void run() {
        mapLines();
    }

    @Override
    public Boolean call() throws Exception {
        return mapLines();
    }

    /**
     *
     * @return
     */
    private boolean mapLines() {
        Path outputPath = context.getBasePath().resolve(Paths.get(MapReduceDemo.MAP_FOLDER));

        ensureFolderExists(outputPath);

        long threadId = Thread.currentThread().getId();

        // While there are lines to process or files which can produce lines ...
        // Process each line and output intermediate file
        String currentLine=null;
        // Short-circuit logic - Always force the system to check for a line to process. If no line then prevent falling out until all files ingested.
        // The isFileIngestionComplete() method sends a NotifyAll when it returns true for the first time so not an issue for the calling thread.
        // Problem happens if another thread has already gotten false and is heading towards the wait state block
        // Solution: The underlying fileIngestedCount atomic integer is only updated inside a sync-block (might not need atomic for that one now)
        //  this loop does not enter the wait state until checking the condition again inside the same sync-block.
        // Cannot get in before the FileIngestor has flipped flag, so won't get into wait mode.
        while((currentLine = this.lineQueue.poll()) != null || !context.isFileIngestionComplete()) {
                if(currentLine != null) {
                    logger.trace("Starting Processing Line Thread Id={} Line={}",  + threadId, currentLine);
                    writeOrUpdateWordMappingFilesForLine(outputPath, threadId, currentLine);
                    context.lineConsumedCount.incrementAndGet();
                }
                else
                {
                    // Don't waste CPU cycles waiting for line production
                    synchronized (context.monitor)
                    {
                        try {
                            // Checking inside synchronized block. Other code cannot be about to increment counter.
                            if(!context.isFileIngestionComplete())
                                context.monitor.wait();
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    }
                }
            }
        return true;
    }

    private void writeOrUpdateWordMappingFilesForLine(Path outputPath, long threadId, String currentLine) {
        Map<String, Long> histogram = histogramMaker.getHistogramForLine(currentLine);
        for(Map.Entry<String, Long> entry : histogram.entrySet())
        {
            //
            wordSet.add(entry.getKey());

            // Update file for that partition
            writeOrUpdateOneWordMappingFile(outputPath, threadId, entry);
        }
    }

    private void writeOrUpdateOneWordMappingFile(Path outputPath, long threadId, Map.Entry<String, Long> entry) {
        String word = entry.getKey();

        String fileName = word + ".thread." + threadId + ".mp";
        Path wordFilePath = outputPath.resolve(fileName);

        long currentCountForWord = getCurrentCountForWord(wordFilePath);
        currentCountForWord = currentCountForWord + entry.getValue();

        try {
            // Overwrite the file
            Files.write(wordFilePath, Long.toString(currentCountForWord).getBytes());
        } catch (IOException e) {
            logger.error("Error Writing Mapping File Thread={} Word={} ", threadId, word);
        }
    }
}
