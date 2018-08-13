package reduce;

import com.ps.mapreducedemo.MapReduceDemo;
import com.ps.mapreducedemo.reduce.WordReducer;
import com.ps.mapreducedemo.util.IoUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class WordReducerTest {
    @Mock
    private IoUtils ioUtils;
    private MapReduceDemo context;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
        context = new MapReduceDemo();
    }

    /**
     * Mockup files in the mapped folder
     * @param word
     * @param fileCount
     * @return
     */
    private List<Path> mockFilesForWord(String word, int fileCount){
        List<Path> filePaths = new ArrayList<>();
        for(int fileIndex=1;fileIndex<=fileCount;fileIndex++){
            filePaths.add(Paths.get(word+"."+fileIndex+".mp"));
        }
        when(ioUtils.getSubPaths(any(), startsWith(word+"."))).thenReturn(filePaths);
        return filePaths;
    }

    @Test
    public void when_3_words_2_files_gets_correct_counts() throws IOException {
        // Mockup words to reduce
        Queue<String> wordQueue = new ConcurrentLinkedQueue<>();
        wordQueue.add("One");
        wordQueue.add("3");
        wordQueue.add("Second");

        // Mockup the mapped folder
        Path mapPath = Paths.get("MapPath");
        when(ioUtils
                .resolvePath(any(), matches(MapReduceDemo.MAP_FOLDER))).thenReturn(mapPath);

        List<Path> pathsOne = mockFilesForWord("One", 2);
        List<Path> paths3 = mockFilesForWord("3", 1);
        List<Path> pathsSecond = mockFilesForWord("Second", 0);

        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("One.1"))))
                .thenReturn(2l);
        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("One.2"))))
                .thenReturn(3l);
        when(ioUtils
                .getCountFromFile(
                        argThat(new PathMatcher("3.1"))))
                .thenReturn(10l);

        // Mockup the reduced folder
        Path reducePath = Paths.get("ReducePath");
        when(ioUtils
                .resolvePath(any(), matches(MapReduceDemo.REDUCE_FOLDER))).thenReturn(reducePath);

        when(ioUtils
                .getSubPaths(any(), matches("One"))).thenReturn(pathsOne);
        when(ioUtils
                .getSubPaths(any(), matches("3"))).thenReturn(paths3);
        when(ioUtils
                .getSubPaths(any(), matches("Second"))).thenReturn(pathsSecond);

        WordReducer sut = new WordReducer(wordQueue, new MapReduceDemo(), ioUtils);
        sut.run();

        Mockito.verify(ioUtils).getSubPaths(any(), matches("Second.*.mp"));
        Mockito.verify(ioUtils).getSubPaths(any(), matches("3.*.mp"));
        Mockito.verify(ioUtils).getSubPaths(any(), matches("One.*.mp"));

        Mockito.verify(ioUtils).getCountFromFile(argThat((arg) -> new PathMatcher("One.1.mp").matches(arg)));
        Mockito.verify(ioUtils).getCountFromFile(argThat((arg) -> new PathMatcher("One.2.mp").matches(arg)));
        Mockito.verify(ioUtils).getCountFromFile(argThat((arg) -> new PathMatcher("3.1.mp").matches(arg)));

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("ReducePath\\One.5.cnt").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                    @Override
                    public boolean matches(String s) {
                        return context.equals(s);
                    }
                }.matches("5")
                )
        );

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("ReducePath\\3.10.cnt").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("10")
                )
        );

        Mockito.verify(ioUtils).overwriteFile(
                argThat((arg) -> new PathMatcher("ReducePath\\Second.0.cnt").matches(arg)),
                argThat((context) -> new ArgumentMatcher<String>() {
                            @Override
                            public boolean matches(String s) {
                                return context.equals(s);
                            }
                        }.matches("0")
                )
        );
    }

    public class PathMatcher implements ArgumentMatcher<Path> {

        private String leftFileName;
        public PathMatcher(String leftFileName) {
            this.leftFileName = leftFileName;
        }

        @Override
        public boolean matches(Path rightPath) {
            if(rightPath == null || leftFileName == null)
                return false;
            String rightFileName = rightPath.toString();
            return rightFileName.startsWith(leftFileName);
        }
    }
}
