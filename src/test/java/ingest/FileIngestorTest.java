package ingest;

import com.ps.mapreducedemo.MapReduceState;
import com.ps.mapreducedemo.ingest.FileIngestor;
import com.ps.mapreducedemo.util.FileUtils;
import com.ps.mapreducedemo.util.PathUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import util.FileSplitterTest;

import java.io.File;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;


public class FileIngestorTest {
    private static Path basePath=null;
    private FileIngestor sut;
    private MapReduceState mapReduceState;

    @BeforeClass
    public static void setupEnv()
    {
        ClassLoader classLoader = FileSplitterTest.class.getClassLoader();
        File file = new File(classLoader.getResource("inputFiles/OneLine.txt").getFile());
        basePath = file.getParentFile().toPath();
    }

    @Before
    public void setup(){
        mapReduceState = new MapReduceState();
        sut = new FileIngestor(mapReduceState, new FileUtils());
    }

    @Test
    public void load_No_Files_And_Nothing_Changes(){
        assertThat(mapReduceState.isFileIngestionComplete(), equalTo(false));
        assertThat(mapReduceState.getCountFilesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountLinesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountLinesProduced(), equalTo(0));
        assertThat(mapReduceState.getCountLinesConsumed(), equalTo(0));
        assertThat(mapReduceState.wereAllLinesConsumed(), equalTo(false));
        sut.run();
        assertThat(mapReduceState.isFileIngestionComplete(), equalTo(false));
        assertThat(mapReduceState.getCountFilesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountRemainingRegisteredFiles(), equalTo(0));
        assertThat(mapReduceState.getCountLinesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountLinesProduced(), equalTo(0));
        assertThat(mapReduceState.getCountLinesConsumed(), equalTo(0));
        assertThat(mapReduceState.wereAllLinesConsumed(), equalTo(false));
    }

    @Test
    public void loads_3_Files_4_Lines_Correctly(){
        PathUtils.loadInputFilePathsIntoQueue(basePath,mapReduceState);
        assertThat(mapReduceState.isFileIngestionComplete(), equalTo(false));
        assertThat(mapReduceState.getCountFilesQueuedForProcessing(), equalTo(3));
        assertThat(mapReduceState.getCountLinesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountLinesProduced(), equalTo(0));
        assertThat(mapReduceState.getCountLinesConsumed(), equalTo(0));
        assertThat(mapReduceState.wereAllLinesConsumed(), equalTo(false));
        sut.run();
        assertThat(mapReduceState.isFileIngestionComplete(), equalTo(true));
        assertThat(mapReduceState.getCountFilesQueuedForProcessing(), equalTo(0));
        assertThat(mapReduceState.getCountRemainingRegisteredFiles(), equalTo(0));
        assertThat(mapReduceState.getCountLinesQueuedForProcessing(), equalTo(4));
        assertThat(mapReduceState.getCountLinesProduced(), equalTo(4));
        assertThat(mapReduceState.getCountLinesConsumed(), equalTo(0));
        assertThat(mapReduceState.wereAllLinesConsumed(), equalTo(false));
    }
}
