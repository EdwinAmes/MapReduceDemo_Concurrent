package com.ps.mapreducedemo.ingest;

import com.ps.mapreducedemo.MapReduceState;
import com.ps.mapreducedemo.util.FileSplitter;
import com.ps.mapreducedemo.MapReduceProcessor;
import com.ps.mapreducedemo.util.IoUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.List;

/**
 * Created by Edwin on 4/21/2016.
 */
public class FileIngestor extends MapReduceProcessor {
    private FileSplitter fileSplitter;
    private MapReduceState mapReduceState;

    static Logger logger = LogManager.getLogger(FileIngestor.class);

    public FileIngestor(MapReduceState mapReduceState, IoUtils ioUtils) {
        super(ioUtils);
        fileSplitter = new FileSplitter(ioUtils);
        this.mapReduceState = mapReduceState;
    }

    @Override
    public void run() {
        ingestFiles();
    }

    @Override
    public Boolean call() throws Exception {
        return ingestFiles();
    }

    /**
     * Ingests all files in target folder.
     * Demo does not distinguish between them.
     * System will generate word counts for all input files combined
     *
     * @return
     */
    private boolean ingestFiles() {
        long threadId = Thread.currentThread().getId();

        logger.trace("Before Processing Files - Thread Id: {} Files In Queue: {}", threadId,
                mapReduceState.getCountFilesQueuedForProcessing());

        // Process each file and put its lines on the queue
        Path currentFile;
        while ((currentFile = mapReduceState.popNextFileFromQueue()) != null) {
            ingestOneFile(threadId, currentFile);
            //
            synchronized (mapReduceState.MONITOR) {
                // Record that another file was loaded
                // Synchronized to prevent LineMapper thread
                //  from checking for completion while count is changing.
                mapReduceState.notifyOneFileIngested();
                // Wake up any waiting LineMapper threads to process the lines
                mapReduceState.MONITOR.notifyAll();
            }
        }
        return true;
    }

    private void ingestOneFile(long threadId, Path currentFile) {
        logger.trace("Starting Processing File Thread Id={} Name={} Files In Queue={}",
                threadId, currentFile.getFileName(), mapReduceState.getCountFilesQueuedForProcessing());

        List<String> lineList = fileSplitter.split(currentFile);

        lineList.forEach(line -> {
            mapReduceState.addLineToQueue(line);
        });
    }
}
