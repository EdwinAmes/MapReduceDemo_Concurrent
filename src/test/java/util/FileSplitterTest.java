package util;

import com.ps.mapreducedemo.util.FileSplitter;
import com.ps.mapreducedemo.util.IoUtils;
import com.ps.mapreducedemo.util.IoUtilsImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

/**
 * Created by Edwin on 4/20/2016.
 */
public class FileSplitterTest {
    @BeforeClass
    public static void setupEnv()
    {
        ClassLoader classLoader = FileSplitterTest.class.getClassLoader();
        File file = new File(classLoader.getResource("inputFiles/OneLine.txt").getFile());
        basePath = file.getParentFile().toPath();
    }

    private static Path basePath=null;

    private FileSplitter sut = null;

    @Before
    public void setup()
    {
        sut = new FileSplitter(new IoUtilsImpl());
    }

    @Test
    public void when_file_does_not_exist_returns_empty_optional()
    {

        List<String> fileLines = sut.split(Paths.get("Does not Exist"));
        assertThat(fileLines.size(), is(equalTo(0)));
    }

    @Test
    public void when_file_has_no_lines_returns_empty_list()
    {
        Path emptyFilePath = basePath.resolve(Paths.get("Empty_NoLines.txt"));
        List<String> fileLines = sut.split(emptyFilePath);
        assertThat(fileLines.size(), is(equalTo(0)));
    }

    @Test
    public void when_file_has_one_line_returns_list_with_line()
    {
        Path oneLineFile = basePath.resolve(Paths.get("OneLine.txt"));
        List<String> fileLines = sut.split(oneLineFile);
        assertThat(fileLines.size(), is(equalTo(1)));
        assertThat(fileLines.get(0), is(equalTo("This file has one line")));
    }

    @Test
    public void when_file_has_three_lines_returns_list_all_lines()
    {
        Path threeLineFile = basePath.resolve(Paths.get("ThreeLine.txt"));
        List<String> fileLines = sut.split(threeLineFile);
        List<String> result = fileLines;
        assertThat(result.size(), is(equalTo(3)));

        // Don't want to demand ordering as this is not needed for the map-reduce demo
        assertThat(result, containsInAnyOrder("has", "This file", "3 lines"));
    }
}
