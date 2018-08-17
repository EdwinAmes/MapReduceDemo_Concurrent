package map;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.MapReduceState;
import com.ps.mapreducedemo.map.LineMapper;
import com.ps.mapreducedemo.util.IoUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import util.PathMatcher;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class LineMapperTest {
    @Mock
    private IoUtils ioUtils;
    private MapReduceState mapReduceState;
    private MapReduceDemo context;
    private LineMapper sut;
    private Path mapPath;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        mapReduceState = new MapReduceState();
        context = new MapReduceDemo();
        sut = new LineMapper(mapReduceState, context, ioUtils);

        // Mockup the mapped folder
        mapPath = Paths.get("MapPath");
        when(ioUtils
                .resolvePath(any(), matches(MapReduceDemo.MAP_FOLDER))).thenReturn(mapPath);
    }

    @Test
    public void when_3_lines_2_words_generates_6_files_with_counts() throws IOException {
        mapReduceState.addLineToQueue("one two");
        mapReduceState.addLineToQueue("two two");
        mapReduceState.addLineToQueue("two");

        //Simulate loading a single file
        mapReduceState.addFileToInputQueue(Paths.get("DummyPath"));
        mapReduceState.notifyOneFileIngested();
        // Flag controls while thread while loop and must be true for code to end
        MatcherAssert.assertThat(mapReduceState.isFileIngestionComplete(), equalTo(true));

        // TODO: Setup mapreducestate with fileIngestionComplete == true
        sut.run();

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("MapPath\\one.line.1.mp").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("1")
                )
        );

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("MapPath\\two.line.1.mp").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("1")
                )
        );

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("MapPath\\two.line.2.mp").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("2")
                )
        );

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("MapPath\\two.line.3.mp").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("1")
                )
        );
    }
}
