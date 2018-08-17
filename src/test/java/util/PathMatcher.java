package util;

import org.mockito.ArgumentMatcher;

import java.nio.file.Path;

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
