package com.ps.mapreducedemo.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Edwin on 4/20/2016.
 * Counts the words
 */
public class LineHistogramMaker {
    Pattern matchNonLettersPattern = Pattern.compile("\\p{Punct}");

    public Map<String, Long> getHistogramForLine(String line)
    {
        line = matchNonLettersPattern.matcher(line).replaceAll("");
        Map<String, Long> histogram = Arrays.stream(line.split(" "))
                .filter(Objects::nonNull)
                .map(word -> word.trim().toLowerCase())
                .filter(word -> {return !word.equals("");})
                .collect(
                        Collectors.groupingBy(Function.identity(), Collectors.counting())
                );

        return histogram;
    }
}
