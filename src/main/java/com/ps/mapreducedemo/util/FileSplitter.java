package com.ps.mapreducedemo.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by Edwin on 4/20/2016.
 * Pull a text file apart into separate lines. Assumes exclusive file access.
 * Trims leading and trailing spaces from all lines
 */
public class FileSplitter {
    static Logger logger = LogManager.getLogger(FileSplitter.class);

    private FileUtils fileUtils;
    public FileSplitter(FileUtils fileUtils) {
        this.fileUtils = fileUtils;
    }

    /**
     * Convenience Method takes path as string
     * @param fileNameFullPath
     * @return Lines in file if file is found and readable
     */
    public Optional<List<String>> split(String fileNameFullPath)
    {
        return split(Paths.get(fileNameFullPath));
    }

    /**
     *
     * @param path Path to file to process
     * @return Lines in file if file is found and readable
     */
    public Optional<List<String>> split(Path path)
    {
        List<String> linesInFile = null;

        try{
            path = path.toRealPath(LinkOption.NOFOLLOW_LINKS);
            try {
                // Using in demo for simplicity.
                // Note that readAllLines returns a materialized list and not an iterable. Not suitable for very large files.
                linesInFile = Files.readAllLines(path);
                // Trim lines in file. Creates a new list and makes existing one eligible for GC. Not suitable for large lists.
                linesInFile = linesInFile.stream().filter(line -> {return line != null;}).map(line->line.trim()).collect(Collectors.toList());
            } catch (IOException e) {
                logger.error("File Not Found", e);
            }
        }
        catch(IOException e)
        {
            logger.error("File Read Error", e);
        }

        return Optional.ofNullable(linesInFile);
    }
}
