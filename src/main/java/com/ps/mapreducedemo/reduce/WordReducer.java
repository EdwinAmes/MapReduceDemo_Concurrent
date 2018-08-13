package com.ps.mapreducedemo.reduce;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.MapReduceProcessor;
import com.ps.mapreducedemo.util.IoUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;

/**
 * Created by Edwin on 4/21/2016.
 * Reduces mapped files: sums the mapped files into final word count files - one per word/partition.
 */
public class WordReducer extends MapReduceProcessor {
    static Logger logger = LogManager.getLogger(WordReducer.class);

    Queue<String> wordQueue;
    MapReduceDemo context;

    public WordReducer(Queue<String> wordQueue, MapReduceDemo context, IoUtils ioUtils) {
        super(ioUtils);
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

        Path inputPath = ioUtils.resolvePath(context.loadBasePath(), MapReduceDemo.MAP_FOLDER);
        if(inputPath==null){
            return false;
        }
        ioUtils.ensureFolderExists(inputPath);

        Path outputPath = ioUtils.resolvePath(context.loadBasePath(), MapReduceDemo.REDUCE_FOLDER);
        if(outputPath==null){
            return false;
        }
        ioUtils.ensureFolderExists(outputPath);

        String currentWord=null;
        while((currentWord = this.wordQueue.poll()) != null)
        {
            long totalCountForCurrentWord = sumWordCounts(currentWord, inputPath);

            try {
                ioUtils.overwriteFile(
                        outputPath.resolve(currentWord+"."+totalCountForCurrentWord+".cnt"),
                        Long.toString(totalCountForCurrentWord));
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
        List<Path> partitionFilePathList = ioUtils.getSubPaths(inputPath, word + ".*.mp");
        for(Path partitionFilePath : partitionFilePathList)
        {
            totalCountForWord = totalCountForWord + ioUtils.getCountFromFile(partitionFilePath);
        }
        return totalCountForWord;
    }
}
