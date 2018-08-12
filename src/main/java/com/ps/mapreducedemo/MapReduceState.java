package com.ps.mapreducedemo;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MapReduceState {
    // Synchronize threads
    public static Object MONITOR = new Object();
    private final AtomicInteger fileRegisteredCount = new AtomicInteger(0);
    private final AtomicInteger fileIngestedCount = new AtomicInteger(0);
    private final AtomicBoolean fileIngestionComplete = new AtomicBoolean(false);
    private final AtomicInteger lineProducedCount = new AtomicInteger(0);
    private final AtomicInteger lineConsumedCount = new AtomicInteger(0);
    // Threadsafe collections and counters
    private Queue<Path> fileQueue = new ConcurrentLinkedQueue<Path>();
    private Queue<String> lineQueue = new ConcurrentLinkedQueue<String>();
    //   Partitions. For word-count demo using the words as the partitions.
    private Set<String> wordSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * No need to synchronize.
     * Worst case is returns a larger count which causes another loop
     * @return
     */
    public int getCountRemainingRegisteredFiles() {
        return fileRegisteredCount.get() - fileIngestedCount.get();
    }

    /**
     * Check if all registered files have been processed.
     * @return
     */
    public boolean isFileIngestionComplete()
    {
        if(fileRegisteredCount.get()==0){
            // Define no files loaded as not complete
            return false;
        }
        if(!fileIngestionComplete.get())
        {
            boolean stateFlipped = justLoadedLastFile();
            if(stateFlipped)
            {
                // Wakeup any threads waiting on lines
                synchronized (MONITOR)
                {
                    MONITOR.notifyAll();
                }
            }
        }
        return fileIngestionComplete.get();
    }

    public boolean justLoadedLastFile(){
        return fileIngestionComplete.compareAndSet(false,(getCountRemainingRegisteredFiles() < 1));
    }

    public int getCountFilesQueuedForProcessing(){
        return fileQueue.size();
    }

    public void addFileToInputQueue(Path inputFile){
        fileQueue.add(inputFile);
        fileRegisteredCount.incrementAndGet();
    }

    public Path popNextFileFromQueue(){
        return fileQueue.poll();
    }

    public void notifyOneFileIngested(){
        fileIngestedCount.incrementAndGet();
    }

    public int getCountLinesQueuedForProcessing(){
        return lineQueue.size();
    }

    public int getCountLinesProduced(){
        return lineProducedCount.get();
    }

    public int getCountLinesConsumed(){
        return lineConsumedCount.get();
    }

    public boolean wereAllLinesConsumed(){
        if(lineProducedCount.get()==0){
            return false;
        }
        return lineConsumedCount.get() == lineProducedCount.get();
    }

    public void addLineToQueue(String lineText){
        lineQueue.add(lineText);
        lineProducedCount.incrementAndGet();
    }

    public String popNextLineFromQueue(){
        return lineQueue.poll();
    }

    public void notifyOneLineConsumed(){
        lineConsumedCount.incrementAndGet();
    }

    public void addWord(String word){

        wordSet.add(word);
    }

    /**
     * // Load Set (needed for uniqueness constraint) into Queue (works better for multithreading)
     * @return
     */
    public Queue<String> getWordQueueToProcess(){
        Queue<String> wordQueue = new ConcurrentLinkedQueue<>();
        for(String word : wordSet)
        {
            wordQueue.add(word);
        }
        return wordQueue;
    }
}
