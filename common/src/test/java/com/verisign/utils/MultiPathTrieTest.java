package com.verisign.utils;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by bperroud on 12/22/15.
 */
public class MultiPathTrieTest {

    @Test
    public void testAddOrAppendPath() throws Exception {

        MultiPathTrie<String> multiPathTrie = new MultiPathTrie<>();

        multiPathTrie.addOrAppendPath("/a/b/c", "abc1");
        multiPathTrie.addOrAppendPath("/a/b/c", "abc2");
        multiPathTrie.addOrAppendPath("/a/b", "ab1");

        Collection<String> strings1 = multiPathTrie.get("/a/b/c");

        Assert.assertEquals(2, strings1.size());
        Assert.assertArrayEquals(new String[] { "abc1", "abc2" }, strings1.toArray());

        Collection<String> strings2 = multiPathTrie.get("/a");
        Assert.assertNull(strings2);

        Collection<String> strings3 = multiPathTrie.get("/a/b/c/d");
        Assert.assertNull(strings2);

        Map.Entry<String, Collection<String>> maxPrefix1 = multiPathTrie.findMaxPrefix("/a");
        Assert.assertNull(maxPrefix1);

        Map.Entry<String, Collection<String>> maxPrefix2 = multiPathTrie.findMaxPrefix("/a/b/c/d");
        Assert.assertEquals(
                new AbstractMap.SimpleEntry<String, Collection<String>>("/a/b/c", Arrays.asList(new String[] { "abc1", "abc2" })),
                        maxPrefix2);

        Collection<Map.Entry<String, Collection<String>>> allMatchingPrefix = multiPathTrie.findAllMatchingPrefix("/a/b/c/d");
        Iterator<Map.Entry<String, Collection<String>>> iterator = allMatchingPrefix.iterator();
        Assert.assertEquals(2, allMatchingPrefix.size());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(new AbstractMap.SimpleEntry<String, Collection<String>>("/a/b", Arrays.asList(new String[] { "ab1" })),
                iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(new AbstractMap.SimpleEntry<String, Collection<String>>("/a/b/c", Arrays.asList(new String[] { "abc1", "abc2" })),
                iterator.next());
    }
}