package com.ps.mapreducedemo.util;

import com.ps.mapreducedemo.MapReduceState;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Introduced interface to support mocking with Mockito
 */
public interface IoUtils {
    long getCountFromFile(Path wordFilePath);
    void ensureFolderExists(Path folderPath);
    boolean overwriteFile(Path filePath, String content) throws IOException;

    String readFileContents(Path filePath) throws IOException;

    List<String> readAllLinesInFile(Path filePath);
    void cleanDirectory(Path pathToClean) throws IOException;

    // Path related functionality
    Path loadBasePath(String rootPath);
    /**
     * Loads first level of child paths. Initially used to load a path for all files in a folder.
     * @param path
     */
    List<Path> getSubPaths(Path path, String filter);

    void loadInputFilePathsIntoQueue(Path inputPath, MapReduceState mapReduceState);
    Path resolvePath(Path basePath, String subPath);
}
