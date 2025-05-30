package com.sumit.rdbms.Models;

import java.util.*;

public class TableIndex {
    private static final long serialVersionUID = 1L;

    private String name;
    private String tableName;
    private String columnName;
    private IndexType indexType;
    private Map<Object, List<Integer>> indexMap; // Value -> List of row indices

    public TableIndex(String name, String tableName, String columnName, IndexType indexType) {
        this.name = name;
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexType = indexType;
        this.indexMap = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void clear() {
        indexMap.clear();
    }

    public void addEntry(Object value, int rowIndex) {
        if (!indexMap.containsKey(value)) {
            indexMap.put(value, new ArrayList<>());
        }
        indexMap.get(value).add(rowIndex);
    }

    public List<Integer> findRows(Object value) {
        return indexMap.getOrDefault(value, Collections.emptyList());
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
