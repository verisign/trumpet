package com.verisign.utils;

import org.apache.commons.lang.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.util.*;

/**
 */

@NotThreadSafe
public class MultiPathTrie<T> {

    final PathTrie<Collection<T>> delegate = new PathTrie<>();

    public void addOrAppendPath(final String path, final T payload) {
        if (path == null) {
            return;
        }
        Collection<T> currentPayload = delegate.get(path);
        if  (currentPayload == null) {
            currentPayload = new LinkedList<>();
            delegate.addPath(path, currentPayload);
        }
        currentPayload.add(payload);
    }

    /**
     * Return the largest prefix for the input path with the payload,
     * or null it none.
     *
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public Map.Entry<String, Collection<T>> findMaxPrefix(final String path) {
        return delegate.findMaxPrefix(path);
    }

    public Collection<T> get(final String path) {
        if (path == null) {
            return null;
        }
        return delegate.get(path);
    }

    public Collection<Map.Entry<String, Collection<T>>> findAllMatchingPrefix(final String path) {
        if (path == null) {
            return null;
        }
        return delegate.findAllMatchingPrefix(path);
    }
}