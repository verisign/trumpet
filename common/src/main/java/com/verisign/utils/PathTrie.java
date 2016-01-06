package com.verisign.utils;

import org.apache.commons.lang.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.util.*;

/**
 * A class that implements prefix matching for 
 * components of a filesystem path. The trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 * 
 * The elements of the Trie support payload.
 * The Trie does not support deletion.
 * 
 * Heavily inspired from
 * org/apache/zookeeper/common/PathTrie.java
 * 
 */

@NotThreadSafe
public class PathTrie<T> {

    private static final String PATH_SEPARATOR = File.separator;

    /**
     * the root node of PathTrie
     */
    private final TrieNode<T> rootNode;

    static class TrieNode<U> {
        U payload;
        final HashMap<String, TrieNode> children;
        final TrieNode parent;

        /**
         * create a trienode with parent
         * as parameter
         *
         * @param parent the parent of this trienode
         */
        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
        }

        /**
         * get the parent of this node
         *
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }


        /**
         * a property that is set
         * for a node - making it
         * special.
         */
        void setPayload(U payload) {
            this.payload = payload;
        }

        /**
         * the property of this
         * node
         *
         * @return the property for this
         * node
         */
        U getPayload() {
            return this.payload;
        }

        boolean hasPayload() {
            return getPayload() != null;
        }

        /**
         * add a child to the existing node
         *
         * @param childName the string name of the child
         * @param node      the node that is the child
         */
        void addChild(String childName, TrieNode node) {
            if (children.containsKey(childName)) {
                return;
            }
            children.put(childName, node);
        }

        /**
         * return the child of a node mapping
         * to the input childname
         *
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode<U> getChild(String childName) {
            if (!children.containsKey(childName)) {
                return null;
            } else {
                return children.get(childName);
            }
        }

        /**
         * get the list of children of this
         * trienode.
         *
         * @return the string list of its children
         */
        String[] getChildren() {
            return children.keySet().toArray(new String[0]);
        }

        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            for (String str : children.keySet()) {
                sb.append(" " + str);
            }
            return sb.toString();
        }
    }

    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }

    /**
     * add a path to the path trie
     *
     * @param path
     */
    public void addPath(final String path, final T payload) {
        if (path == null) {
            return;
        }
        final String[] pathComponents = splitPath(path);
        TrieNode<T> parent = rootNode;
        String part = null;
        for (int i = 1; i < pathComponents.length; i++) {
            part = pathComponents[i];
            if (StringUtils.isEmpty(part)) {
                continue;
            }
            if (parent.getChild(part) == null) {
                parent.addChild(part, new TrieNode<T>(parent));
            }
            parent = parent.getChild(part);
        }
        parent.setPayload(payload);
    }

    private static String[] splitPath(final String path) {
        final String[] pathComponents = path.split(PATH_SEPARATOR);
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        return pathComponents;
    }

    /**
     * Return the largest prefix for the input path with the payload,
     * or null it none.
     *
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public Map.Entry<String, T> findMaxPrefix(final String path) {
        if (path == null) {
            return null;
        }
        if (PATH_SEPARATOR.equals(path)) {
            return null;
        }
        final String[] pathComponents = splitPath(path);
        TrieNode<T> parent = rootNode;
        final List<String> components = new ArrayList<String>();
        int i = 1;
        String part = null;
        final StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        T lastPayload = null;
        while ((i < pathComponents.length)) {
            if (parent.getChild(pathComponents[i]) != null) {
                part = pathComponents[i];
                parent = parent.getChild(part);
                components.add(part);
                if (parent.hasPayload()) {
                    lastindex = i - 1;
                    lastPayload = parent.getPayload();
                }
            } else {
                break;
            }
            i++;
        }
        if (lastindex == -1 || lastPayload == null) {
            return null;
        }

        for (int j = 0; j < (lastindex + 1); j++) {
            sb.append(PATH_SEPARATOR).append(components.get(j));
        }
        return new java.util.AbstractMap.SimpleEntry<String, T>(sb.toString(), lastPayload);
    }

    public T get(final String path) {
        if (path == null) {
            return null;
        }
        final String[] pathComponents = splitPath(path);
        TrieNode<T> parent = rootNode;
        String part = null;
        for (int i = 1; i < pathComponents.length; i++) {
            part = pathComponents[i];
            if (StringUtils.isEmpty(part)) {
                continue;
            }
            parent = parent.getChild(part);
            if (parent == null) {
                return null;
            }
        }
        return parent.getPayload();
    }

    public Collection<Map.Entry<String, T>> findAllMatchingPrefix(final String path) {
        if (path == null) {
            return null;
        }
        final String[] pathComponents = splitPath(path);
        TrieNode<T> parent = rootNode;
        Collection<Map.Entry<String, T>> result = new LinkedList<>();
        final StringBuilder sb = new StringBuilder();
        String part = null;
        for (int i = 1; i < pathComponents.length; i++) {
            part = pathComponents[i];
            if (StringUtils.isEmpty(part)) {
                continue;
            }
            if (parent.hasPayload()) {
                result.add(new java.util.AbstractMap.SimpleEntry<String, T>(sb.toString(), parent.getPayload()));
            }
            sb.append(PATH_SEPARATOR);
            sb.append(part);
            parent = parent.getChild(part);
            if (parent == null) {
                break;
            }
        }
        return result;
    }
}