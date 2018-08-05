package com.ps.mapreducedemo.ingest;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.util.FileSplitter;
import com.ps.mapreducedemo.MapReduceNodeProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Created by Edwin on 4/21/2016.
 */
public class FileIngestor extends MapReduceNodeProcessor {
    private FileSplitter fileSplitter = new FileSplitter();
    private Queue<Path> fileQueue;
    private Queue<String> lineQueue;
    private MapReduceDemo context;

    static Logger logger = LogManager.getLogger(FileIngestor.class);

    public FileIngestor(Queue<Path> fileQueue, Queue<String> lineQueue, MapReduceDemo context) {
        this.lineQueue = lineQueue;
        this.fileQueue = fileQueue;
        this.context = context;
    }

    @Override
    public void run()
    {
        ingestFiles();
    }

    @Override
    public Boolean call() throws Exception {
        return ingestFiles();
    }

    /**
     * Ingests all files in target folder.
     * Demo does not distinguish between them.
     *  System will generate word counts for all input files combined
     * @return
     */
    private boolean ingestFiles() {
        long threadId = Thread.currentThread().getId();

        logger.trace("Before Processing Files - Thread Id: {} Files In Queue: {}", threadId,  this.fileQueue.size());

        // Process each file and put its lines on the queue
        Path currentFile;
        while((currentFile = this.fileQueue.poll()) != null) {
            ingestOneFile(threadId, currentFile);
            //
            synchronized (context.monitor)
            {
                // Record that another file was loaded
                // Synchronized to prevent LineMapper thread
                //  from checking for completion while count is changing.
                context.fileIngestedCount.incrementAndGet();
                // Wake up any waiting LineMapper threads to process the lines
                context.monitor.notifyAll();
            }
        }
        return true;
    }

    private void ingestOneFile(long threadId, Path currentFile) {
        logger.trace("Starting Processing File Thread Id={} Name={} Files In Queue={}", threadId, currentFile.getFileName(), this.fileQueue.size());

        Optional<List<String>> optionalLineList = fileSplitter.split(currentFile);
        if(optionalLineList.isPresent())
        {
            optionalLineList.get().forEach(line -> {
                this.lineQueue.add(line);
                this.context.lineProducedCount.incrementAndGet();
            });
        }
    }
}
