package com.verisign.utils;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.AbstractMap;

/**
 * Created by bperroud on 12/19/15.
 */
public class PathTrieTest {

    @Test
    public void test() throws Exception {

        PathTrie<String> pathTrie = new PathTrie<>();

        pathTrie.addPath("/a", "a");
        pathTrie.addPath("/a/b/c", "abc");

        Assert.assertEquals(new AbstractMap.SimpleEntry<>("/a/b/c", "abc"), pathTrie.findMaxPrefix("/a/b/c/d"));
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("/a", "a"), pathTrie.findMaxPrefix("/a/b"));
        Assert.assertNull(pathTrie.findMaxPrefix("/b"));
        Assert.assertEquals("abc", pathTrie.get("/a/b/c"));
        Assert.assertNull(pathTrie.get("/a/b"));
        System.out.println(pathTrie.findAllMatchingPrefix("/a/b/c/d"));

    }
}