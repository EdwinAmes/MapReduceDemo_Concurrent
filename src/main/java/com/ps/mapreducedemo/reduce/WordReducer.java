package com.ps.mapreducedemo.reduce;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.MapReduceNodeProcessor;
import com.ps.mapreducedemo.util.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Queue;

import static com.ps.mapreducedemo.util.PathUtils.getSubPaths;

/**
 * Created by Edwin on 4/21/2016.
 * Reduces mapped files: sums the mapped files into final word count files - one per word/partition.
 */
public class WordReducer extends MapReduceNodeProcessor {
    static Logger logger = LogManager.getLogger(WordReducer.class);

    Queue<String> wordQueue;
    MapReduceDemo context;

    public WordReducer(Queue<String> wordQueue, MapReduceDemo context, FileUtils fileUtils) {
        super(fileUtils);
        this.wordQueue = wordQueue;
        this.context = context;
    }

    @Override
    public void run() {
        reducePartition();
    }

    @Override
    public Boolean call() throws Exception {
        return reducePartition();
    }

    /**
     * Processes all partitions until finished
     * @return
     */
    private boolean reducePartition()
    {
        long threadId = Thread.currentThread().getId();

        Path inputPath = context.loadBasePath().resolve(Paths.get(MapReduceDemo.MAP_FOLDER));
        ensureFolderExists(inputPath);

        Path outputPath = context.loadBasePath().resolve(Paths.get(MapReduceDemo.REDUCE_FOLDER));
        ensureFolderExists(outputPath);

        String currentWord=null;
        while((currentWord = this.wordQueue.poll()) != null)
        {
            long totalCountForCurrentWord = sumWordCounts(currentWord, inputPath);

            try {
                // Overwrite the file
                Files.write(outputPath.resolve(currentWord+"."+totalCountForCurrentWord+".cnt"), Long.toString(totalCountForCurrentWord).getBytes());
            } catch (IOException e) {
                logger.error("Error Writing Reduce File Thread={} Word={} ", threadId, currentWord);
            }
        }
        return true;
    }

    /**
     * Read all partition file for specified word. Add up the word counts.
     * @param word
     * @param inputPath
     */
    private long sumWordCounts(String word, Path inputPath) {
        long totalCountForWord = 0;
        List<Path> partitionFileList = getSubPaths(inputPath, word + ".*.mp");
        for(Path partitionFile : partitionFileList)
        {
            totalCountForWord = totalCountForWord + getCurrentCountForWord(partitionFile);
        }
        return totalCountForWord;
    }
}
