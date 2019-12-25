package com.act.Utils;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MonitorDirUtilPartitionedQueue {

    private static final Logger logger = Logger.getLogger(MonitorDirUtilPartitionedQueue.class);

    private final File spoolDirectory;
    private final String completedSuffix;
    private final Pattern includePattern;
    private final Pattern ignorePattern;
    private final boolean recursiveDirectorySearch;
    private final Path spoolDirPath;


    public static MonitorDirUtilPartitionedQueue.Builder builder(){
        return new Builder();
    }

    private MonitorDirUtilPartitionedQueue(File spoolDirectory, String completedSuffix, String includePattern, String ignorePattern, boolean recursiveDirectorySearch){

        // Sanity checks
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(includePattern);
        Preconditions.checkNotNull(ignorePattern);

        // Verify directory exists and is readable/writable
        Preconditions.checkState(spoolDirectory.exists(),
                "Directory does not exist: " + spoolDirectory.getAbsolutePath());
        Preconditions.checkState(spoolDirectory.isDirectory(),
                "Path is not a directory: " + spoolDirectory.getAbsolutePath());

        this.spoolDirectory = spoolDirectory;
        this.completedSuffix = completedSuffix;
        this.includePattern = Pattern.compile(includePattern);
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.recursiveDirectorySearch = recursiveDirectorySearch;

        spoolDirPath = Paths.get(spoolDirectory.getAbsolutePath());

    }


    /**
     * Recursively gather candidate files
     *
     * @param directory
     *            the directory to gather files from
     * @return list of files within the passed in directory
     */
    public List<File> getCandidateFiles(Path directory) {
        Preconditions.checkNotNull(directory);
        final List<File> candidateFiles = new ArrayList<>();
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (directory.equals(dir)) { // The top directory should always be listed
                        return FileVisitResult.CONTINUE;
                    }
                    String directoryName = dir.getFileName().toString();
                    if (!recursiveDirectorySearch || directoryName.startsWith(".")
                            || ignorePattern.matcher(directoryName).matches()) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path candidate, BasicFileAttributes attrs) throws IOException {
                    String fileName = candidate.getFileName().toString();
                    if (!fileName.endsWith(completedSuffix) && !fileName.startsWith(".") && includePattern.matcher(fileName).matches()
                            && !ignorePattern.matcher(fileName).matches()) {

                        File originalFile = candidate.toFile();
                        File loadingFile = new File(originalFile.getAbsolutePath() + ".loading" );
                        originalFile.renameTo(loadingFile);
                        candidateFiles.add(loadingFile);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.error("I/O exception occurred while listing directories. "
                    + "Files already matched will be returned. " + directory, e);
        }

        return candidateFiles;
    }

    /**
     * Special builder class for MonitorDirUtil
     */
    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix = ".COMPLETED";
        private String includePattern = "^.*$"; //all files
        private String ignorePattern =  "^$"; //no effect
        private boolean recursiveDirectorySearch = false;

        public Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public Builder includePattern(String includePattern) {
            this.includePattern = includePattern;
            return this;
        }

        public Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public Builder recursiveDirectorySearch(boolean recursiveDirectorySearch) {
            this.recursiveDirectorySearch = recursiveDirectorySearch;
            return this;
        }


        public MonitorDirUtilPartitionedQueue build() {
           return new MonitorDirUtilPartitionedQueue(spoolDirectory, completedSuffix, includePattern, ignorePattern, recursiveDirectorySearch);
        }
    }


}
