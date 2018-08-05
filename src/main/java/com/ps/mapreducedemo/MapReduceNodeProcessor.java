package com.ps.mapreducedemo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Created by Edwin on 4/21/2016.
 */
public abstract class MapReduceNodeProcessor implements Runnable, Callable<Boolean> {

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
                String currentCountAsString = new String(Files.readAllBytes(wordFilePath));
                currentCountForWord = Long.parseLong(currentCountAsString);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return currentCountForWord;
    }
}
