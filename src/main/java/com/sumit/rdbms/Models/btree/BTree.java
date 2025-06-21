package com.sumit.rdbms.Models.btree;

import java.io.Serializable;
import java.util.*;

public class BTree<K extends Comparable<? super K>, V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private BTreeNode<K, V> root;
    private final int minDegree;

    public BTree(int minDegree) {
        this.minDegree = minDegree;
        this.root = new BTreeNode<>(minDegree, true);
    }

    // Search for a key
    public V search(K key) {
        if (root == null) {
            return null;
        }
        return root.search(key);
    }

    // Insert a key-value pair
    public void insert(K key, V value) {
        if (root.isFull()) {
            BTreeNode<K, V> newRoot = new BTreeNode<>(minDegree, false);
            newRoot.getChildren().add(root);
            newRoot.splitChild(0);
            root = newRoot;
        }

        root.insertNonFull(key, value);
    }

    // Range query
    public List<V> findRange(K minKey, K maxKey) {
        List<V> result = new ArrayList<>();
        if (root != null) {
            root.findRange(minKey, maxKey, result);
        }
        return result;
    }

    // Get all values (in-order traversal)
    public List<V> getAllValues() {
        List<V> result = new ArrayList<>();
        if (root != null) {
            inOrderTraversal(root, result);
        }
        return result;
    }

    private void inOrderTraversal(BTreeNode<K, V> node, List<V> result) {
        int i = 0;
        while (i < node.getKeys().size()) {
            if (!node.isLeaf()) {
                inOrderTraversal(node.getChildren().get(i), result);
            }
            result.add(node.getValues().get(i));
            i++;
        }

        if (!node.isLeaf()) {
            inOrderTraversal(node.getChildren().get(i), result);
        }
    }
}