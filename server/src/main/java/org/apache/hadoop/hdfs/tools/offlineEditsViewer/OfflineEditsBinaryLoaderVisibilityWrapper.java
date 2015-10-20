package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;

import java.io.IOException;

/**
 * This class is only here to increase the visibility for
 * {@see org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsBinaryLoader}
 */
public class OfflineEditsBinaryLoaderVisibilityWrapper implements OfflineEditsLoader {

    private final OfflineEditsLoader wrapped;

    public OfflineEditsBinaryLoaderVisibilityWrapper(OfflineEditsVisitor visitor,
                                                     EditLogInputStream inputStream, OfflineEditsViewer.Flags flags) {
        this.wrapped = new OfflineEditsBinaryLoader(visitor, inputStream, flags);
    }

    @Override
    public void loadEdits() throws IOException {
        wrapped.loadEdits();
    }
}
