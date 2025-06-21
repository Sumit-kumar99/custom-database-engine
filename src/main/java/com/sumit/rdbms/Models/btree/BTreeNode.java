package com.sumit.rdbms.Models.btree;


import java.io.Serializable;
import java.util.*;

public class BTreeNode<K extends Comparable<? super K>, V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int minDegree; // Minimum degree (t)
    private final int maxKeys;   // Maximum number of keys (2t - 1)
    private final int maxChildren; // Maximum number of children (2t)

    private List<K> keys;
    private List<V> values;
    private List<BTreeNode<K, V>> children;
    private boolean isLeaf;

    public BTreeNode(int minDegree, boolean isLeaf) {
        this.minDegree = minDegree;
        this.maxKeys = 2 * minDegree - 1;
        this.maxChildren = 2 * minDegree;
        this.isLeaf = isLeaf;

        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    // Getters
    public int getMinDegree() { return minDegree; }
    public int getMaxKeys() { return maxKeys; }
    public int getMaxChildren() { return maxChildren; }
    public List<K> getKeys() { return keys; }
    public List<V> getValues() { return values; }
    public List<BTreeNode<K, V>> getChildren() { return children; }
    public boolean isLeaf() { return isLeaf; }
    public void setLeaf(boolean leaf) { this.isLeaf = leaf; }

    // Check if node is full
    public boolean isFull() {
        return keys.size() == maxKeys;
    }

    // Check if node has minimum keys
    public boolean hasMinimumKeys() {
        return keys.size() >= minDegree - 1;
    }

    // Search for a key in this node
    public V search(K key) {
        int i = 0;
        while (i < keys.size() && key.compareTo(keys.get(i)) > 0) {
            i++;
        }

        if (i < keys.size() && key.compareTo(keys.get(i)) == 0) {
            return values.get(i);
        }

        if (isLeaf) {
            return null;
        }

        return children.get(i).search(key);
    }

    // Insert a key-value pair when node is not full
    public void insertNonFull(K key, V value) {
        int i = keys.size() - 1;

        if (isLeaf) {
            // Insert into leaf node
            keys.add(null);
            values.add(null);

            while (i >= 0 && key.compareTo(keys.get(i)) < 0) {
                keys.set(i + 1, keys.get(i));
                values.set(i + 1, values.get(i));
                i--;
            }

            keys.set(i + 1, key);
            values.set(i + 1, value);
        } else {
            // Find child to insert into
            while (i >= 0 && key.compareTo(keys.get(i)) < 0) {
                i--;
            }
            i++;

            if (children.get(i).isFull()) {
                splitChild(i);

                if (key.compareTo(keys.get(i)) > 0) {
                    i++;
                }
            }

            children.get(i).insertNonFull(key, value);
        }
    }

    // Split a full child
    public void splitChild(int index) {
        BTreeNode<K, V> fullChild = children.get(index);
        BTreeNode<K, V> newChild = new BTreeNode<>(minDegree, fullChild.isLeaf());

        int midIndex = minDegree - 1;

        // Copy second half of keys and values to new child
        for (int j = 0; j < minDegree - 1; j++) {
            newChild.keys.add(fullChild.keys.get(midIndex + 1 + j));
            newChild.values.add(fullChild.values.get(midIndex + 1 + j));
        }

        // Copy second half of children if not leaf
        if (!fullChild.isLeaf()) {
            for (int j = 0; j < minDegree; j++) {
                newChild.children.add(fullChild.children.get(midIndex + 1 + j));
            }
        }

        // Remove moved keys, values, and children from full child
        for (int j = fullChild.keys.size() - 1; j > midIndex; j--) {
            fullChild.keys.remove(j);
            fullChild.values.remove(j);
        }

        if (!fullChild.isLeaf()) {
            for (int j = fullChild.children.size() - 1; j > midIndex; j--) {
                fullChild.children.remove(j);
            }
        }

        // Move middle key up to parent
        keys.add(index, fullChild.keys.get(midIndex));
        values.add(index, fullChild.values.get(midIndex));
        children.add(index + 1, newChild);

        // Remove middle key from full child
        fullChild.keys.remove(midIndex);
        fullChild.values.remove(midIndex);
    }

    // Find all values for range queries
    public void findRange(K minKey, K maxKey, List<V> result) {
        int i = 0;

        while (i < keys.size()) {
            if (!isLeaf) {
                // Recurse on child before current key
                if (minKey.compareTo(keys.get(i)) < 0) {
                    children.get(i).findRange(minKey, maxKey, result);
                }
            }

            // Check if current key is in range
            if (keys.get(i).compareTo(minKey) >= 0 && keys.get(i).compareTo(maxKey) <= 0) {
                result.add(values.get(i));
            }

            // If current key is greater than maxKey, stop
            if (keys.get(i).compareTo(maxKey) > 0) {
                break;
            }

            i++;
        }

        // Recurse on last child if not leaf
        if (!isLeaf && i < children.size()) {
            children.get(i).findRange(minKey, maxKey, result);
        }
    }
}
