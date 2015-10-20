package com.verisign.vscc.hdfs.trumpet.server.editlog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bperroud on 08-Apr-15.
 */
public class EditLogDir {

    private static Logger LOG = LoggerFactory.getLogger(EditLogDir.class);

    // Regexp from org.apache.hadoop.hdfs.qjournal.serverJNStorage
    private static final Pattern EDIT_LOG_REGEX =
            Pattern.compile(NNStorage.NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
    private static final Pattern EDIT_LOG_INPROGRESS_REGEX =
            Pattern.compile(NNStorage.NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)(?:\\..*)?");
    private static final String CURRENT = "current";
    private static final String[] EMPTY_ARRAY = new String[0];

    private final File editLogDir;

    public EditLogDir(File rootDir, Configuration conf) {
        Preconditions.checkNotNull(rootDir);

        // we need to point to <dfs.nameservices>/current

        File[] editLogDirs = rootDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory()) {
                    File[] currents = pathname.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return CURRENT.equals(name);
                        }
                    });
                    return currents != null && currents.length > 0;
                }
                return false;
            }
        });

        // TODO: if editLogDirs.length > 1, we should ask for namespace.
        Preconditions.checkNotNull(editLogDirs, "No folder matching edit log dir structure in rootDir:" +
                " " + rootDir + "/*/current.");
        Preconditions.checkState(editLogDirs.length == 1, "No folder or too many folders matching edit log dir structure in rootDir:" +
                " " + rootDir + "/*/current.");

        this.editLogDir = new File(editLogDirs[0], CURRENT);

        Preconditions.checkState(this.editLogDir.isDirectory());
        Preconditions.checkState(this.editLogDir.canRead());

        LOG.debug("Found a dfs.*.edits.dir at {}", this.editLogDir);
    }

    public File searchBestMatchingSegment(final long txId) {

        final AtomicReference<BestMatchingFile> bestMatchingFile = new AtomicReference<>(null);
        final SortedMap<Long, String> matchingFilesSortedByTx = new TreeMap<>();

        File[] matchingFiles = editLogDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String fileName) {

                Matcher inprogressMatcher = EDIT_LOG_INPROGRESS_REGEX.matcher(fileName);

                if (inprogressMatcher.matches()) {
                    long startTxId = Long.parseLong(inprogressMatcher.group(1));
                    if (startTxId <= txId) {
                        matchingFilesSortedByTx.put(startTxId, fileName);
                        return true;
                    }
                }

                Matcher editMatcher = EDIT_LOG_REGEX.matcher(fileName);
                if (editMatcher.matches()) {
                    long startTxId = Long.parseLong(editMatcher.group(1));
                    long endTxId = Long.parseLong(editMatcher.group(2));
                    if (startTxId <= txId && endTxId >= txId) {
                        matchingFilesSortedByTx.put(startTxId, fileName);
                        return true;
                    }
                    // corner case: as we're returning the best matching,
                    // if the first edit you're seeing (hint: they are NOT
                    // necessarily sorted) has startTxId bigger
                    // than the txId we're searching for,
                    // it is the best match we can do.
                    if (startTxId > txId && (bestMatchingFile.get() == null || startTxId < bestMatchingFile.get().startTxId)) {
                        bestMatchingFile.set(new BestMatchingFile(startTxId, fileName));
                    }
                }

                return false;

            }
        });

        if (matchingFiles == null) {
            return null;
        } else if (matchingFiles.length == 0) {
            if (bestMatchingFile.get() != null) {
                return new File(editLogDir, bestMatchingFile.get().filename);
            } else {
                return null;
            }
        } else if (matchingFiles.length > 1) {
            // If, for some odd reasons, there's several inprogress files, take the one with
            // the highest txId.
            String filename = matchingFilesSortedByTx.get(matchingFilesSortedByTx.lastKey());
            LOG.debug("Multiple files are matching {}, picking the one with the highest txId, i.e. {}." +
                    " And BTW, you might want to clean up your edit log directory.", Arrays.toString(matchingFiles), filename);
            return new File(editLogDir, filename);
        } else {
            return matchingFiles[0];
        }
    }

    public static Long extractStartTxId(File editLogFile) {

        String fileName = editLogFile.getName();

        Matcher inprogressMatcher = EDIT_LOG_INPROGRESS_REGEX.matcher(fileName);

        if (inprogressMatcher.matches()) {
            return Long.valueOf(inprogressMatcher.group(1));
        }

        Matcher editMatcher = EDIT_LOG_REGEX.matcher(fileName);
        if (editMatcher.matches()) {
            return Long.valueOf(editMatcher.group(1));
        }

        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append('[');
        try {
            sb.append(editLogDir.getCanonicalPath());
        } catch (IOException e) {
            sb.append(e.getMessage());
        }
        sb.append(']');
        return sb.toString();
    }

    public static boolean isInProgress(File editLogFile) {
        return editLogFile != null && EDIT_LOG_INPROGRESS_REGEX.matcher(editLogFile.getName()).matches();
    }

    private static class BestMatchingFile {
        public final long startTxId;
        public final String filename;

        public BestMatchingFile(final long startTxId, final String filename) {
            this.startTxId = startTxId;
            this.filename = filename;
        }
    }
}
