package com.sumit.rdbms.Models;

import com.sumit.rdbms.Models.btree.BTree;

import java.io.Serializable;
import java.util.*;

public class TableIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String tableName;
    private String columnName;
    private IndexType indexType;

    // Hash-based index
    private Map<Object, List<Integer>> hashIndex;

    // B-Tree based index - Now works with wildcard types
    private BTree<Comparable<Object>, List<Integer>> btreeIndex;

    public TableIndex(String name, String tableName, String columnName, IndexType indexType) {
        this.name = name;
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexType = indexType;

        if (indexType == IndexType.HASH) {
            this.hashIndex = new HashMap<>();
        } else if (indexType == IndexType.BTREE) {
            this.btreeIndex = new BTree<>(3); // Now compiles correctly
        }
    }

    // ... rest of the implementation remains the same as before

    // Getters
    public String getName() { return name; }
    public String getTableName() { return tableName; }
    public String getColumnName() { return columnName; }
    public IndexType getIndexType() { return indexType; }

    public void clear() {
        if (indexType == IndexType.HASH) {
            hashIndex.clear();
        } else if (indexType == IndexType.BTREE) {
            btreeIndex = new BTree<>(3);
        }
    }

    @SuppressWarnings("unchecked")
    public void addEntry(Object value, int rowIndex) {
        if (indexType == IndexType.HASH) {
            if (!hashIndex.containsKey(value)) {
                hashIndex.put(value, new ArrayList<>());
            }
            hashIndex.get(value).add(rowIndex);
        } else if (indexType == IndexType.BTREE) {
            if (value instanceof Comparable) {
                Comparable<Object> key = (Comparable<Object>) value;
                List<Integer> existing = btreeIndex.search(key);
                if (existing == null) {
                    existing = new ArrayList<>();
                    btreeIndex.insert(key, existing);
                }
                existing.add(rowIndex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public List<Integer> findRows(Object value) {
        if (indexType == IndexType.HASH) {
            return hashIndex.getOrDefault(value, Collections.emptyList());
        } else if (indexType == IndexType.BTREE) {
            if (value instanceof Comparable) {
                Comparable<Object> key = (Comparable<Object>) value;
                List<Integer> result = btreeIndex.search(key);
                return result != null ? result : Collections.emptyList();
            }
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<Integer> findRowsInRange(Object minValue, Object maxValue) {
        if (indexType == IndexType.BTREE && minValue instanceof Comparable && maxValue instanceof Comparable) {
            Comparable<Object> minKey = (Comparable<Object>) minValue;
            Comparable<Object> maxKey = (Comparable<Object>) maxValue;

            List<List<Integer>> rangeLists = btreeIndex.findRange(minKey, maxKey);
            List<Integer> result = new ArrayList<>();

            for (List<Integer> list : rangeLists) {
                result.addAll(list);
            }

            return result;
        }
        return Collections.emptyList();
    }

    public void rebuildIndex(Table table) {
        clear();
        List<Map<String, Object>> rows = table.getRows();
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object value = row.get(columnName);
            if (value != null) {
                addEntry(value, i);
            }
        }
    }
}