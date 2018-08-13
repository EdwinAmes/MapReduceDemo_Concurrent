package com.ps.mapreducedemo.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Edwin on 4/20/2016.
 * Pull a text file apart into separate lines. Assumes exclusive file access.
 * Trims leading and trailing spaces from all lines
 */
public class FileSplitter {
    static Logger logger = LogManager.getLogger(FileSplitter.class);

    private IoUtils ioUtils;
    public FileSplitter(IoUtils ioUtils) {
        this.ioUtils = ioUtils;
    }

    /**
     *
     * @param path Path to file to process
     * @return Lines in file if file is found and readable
     */
    public List<String> split(Path path)
    {
        List<String> linesInFile = ioUtils.readAllLinesInFile(path).stream()
                    .filter(line -> {return line != null;})
                    .map(line->line.trim())
                    .collect(Collectors.toList());

        return linesInFile;
    }
}
