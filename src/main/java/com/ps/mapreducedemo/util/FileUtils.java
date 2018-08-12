package com.ps.mapreducedemo.util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;


import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class FileUtils {
    // Reading short files with standard numbers
    private static Charset charSet = StandardCharsets.US_ASCII;

    public void overwriteFile(Path filePath, String content) throws IOException {
        try (OutputStream out = new BufferedOutputStream(
                Files.newOutputStream(filePath, WRITE, TRUNCATE_EXISTING))) {
            byte[] contentData = content.getBytes(charSet);
            out.write(contentData, 0, contentData.length);
        }
    }

    public String readFileContents(Path filePath)  throws IOException {
        return new String(Files.readAllBytes(filePath), charSet);
    }

    public List<String> readAllLinesInFile(Path filePath) throws IOException {
        return Files.readAllLines(filePath, charSet);
    }

    public void cleanDirectory(Path pathToClean) throws IOException{
        org.apache.commons.io.FileUtils.cleanDirectory(pathToClean.toFile());
    }
}
