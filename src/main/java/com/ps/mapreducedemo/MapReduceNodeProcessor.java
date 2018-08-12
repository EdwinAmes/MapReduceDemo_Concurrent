package com.ps.mapreducedemo;

import com.ps.mapreducedemo.util.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Created by Edwin on 4/21/2016.
 */
public abstract class MapReduceNodeProcessor implements Runnable, Callable<Boolean> {
    static Logger logger = LogManager.getLogger(MapReduceNodeProcessor.class);
    protected FileUtils fileUtils;
    public MapReduceNodeProcessor(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    protected void ensureFolderExists(Path folderPath) {
        File outputFolder = folderPath.toFile();
        if(!outputFolder.exists())
            outputFolder.mkdir();
    }

    protected long getCurrentCountForWord(Path wordFilePath) {
        long currentCountForWord = 0;
        File wordFile = wordFilePath.toFile();

        if(wordFile.exists())
        {
            try {
                String currentCountAsString = fileUtils.readFileContents(wordFilePath);
                currentCountForWord = Long.parseLong(currentCountAsString);
            } catch (IOException e) {
                logger.error("Unable to read file: {}", wordFilePath);
            }
        }
        return currentCountForWord;
    }
}
