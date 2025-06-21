package com.sumit.rdbms.Models;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private List<Column> columns;
    private List<Map<String, Object>> rows;
    private Map<String, TableIndex> indices;

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        this.rows = new ArrayList<>();
        this.indices = new HashMap<>();
    }

    // Getters and setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public List<Column> getColumns() { return columns; }
    public void setColumns(List<Column> columns) { this.columns = columns; }
    public List<Map<String, Object>> getRows() { return rows; }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
        // Rebuild all indices after row replacement
        for (TableIndex index : indices.values()) {
            index.rebuildIndex(this);
        }
    }

    public void addRow(Map<String, Object> row) {
        int rowIndex = rows.size();
        rows.add(row);

        // Update all indices
        for (TableIndex index : indices.values()) {
            Object value = row.get(index.getColumnName());
            if (value != null) {
                index.addEntry(value, rowIndex);
            }
        }
    }

    public void createIndex(String indexName, String columnName, IndexType indexType) {
        if (indices.containsKey(indexName)) {
            throw new IllegalArgumentException("Index already exists: " + indexName);
        }

        // Verify column exists
        boolean columnExists = columns.stream()
                .anyMatch(column -> column.getName().equals(columnName));

        if (!columnExists) {
            throw new IllegalArgumentException("Column does not exist: " + columnName);
        }

        TableIndex index = new TableIndex(indexName, name, columnName, indexType);
        index.rebuildIndex(this);
        indices.put(indexName, index);
    }

    public void dropIndex(String indexName) {
        if (!indices.containsKey(indexName)) {
            throw new IllegalArgumentException("Index does not exist: " + indexName);
        }
        indices.remove(indexName);
    }

    public List<TableIndex> getIndices() {
        return new ArrayList<>(indices.values());
    }

    public TableIndex getIndex(String indexName) {
        return indices.get(indexName);
    }

    // Enhanced select with B-Tree range query support
    public List<Map<String, Object>> selectWithIndex(Condition condition) {
        if (condition instanceof EqualCondition) {
            return selectWithEqualCondition((EqualCondition) condition);
        } else if (condition instanceof RangeCondition) {
            return selectWithRangeCondition((RangeCondition) condition);
        } else {
            // Fall back to full table scan
            return select(condition);
        }
    }

    private List<Map<String, Object>> selectWithEqualCondition(EqualCondition condition) {
        // Find an appropriate index
        TableIndex matchingIndex = null;
        for (TableIndex index : indices.values()) {
            if (index.getColumnName().equals(condition.getColumn())) {
                matchingIndex = index;
                break;
            }
        }

        if (matchingIndex != null) {
            // Use index for query
            List<Integer> rowIndices = matchingIndex.findRows(condition.getValue());
            List<Map<String, Object>> result = new ArrayList<>();

            for (Integer rowIndex : rowIndices) {
                if (rowIndex < rows.size()) {
                    result.add(new HashMap<>(rows.get(rowIndex)));
                }
            }

            return result;
        } else {
            // Fall back to full table scan
            return select(condition);
        }
    }

    private List<Map<String, Object>> selectWithRangeCondition(RangeCondition condition) {
        // Find a B-Tree index for the column
        TableIndex matchingIndex = null;
        for (TableIndex index : indices.values()) {
            if (index.getColumnName().equals(condition.getColumn()) &&
                    index.getIndexType() == IndexType.BTREE) {
                matchingIndex = index;
                break;
            }
        }

        if (matchingIndex != null) {
            // Use B-Tree index for range query
            List<Integer> rowIndices = matchingIndex.findRowsInRange(
                    condition.getMinValue(), condition.getMaxValue());
            List<Map<String, Object>> result = new ArrayList<>();

            for (Integer rowIndex : rowIndices) {
                if (rowIndex < rows.size()) {
                    result.add(new HashMap<>(rows.get(rowIndex)));
                }
            }

            return result;
        } else {
            // Fall back to full table scan
            return select(condition);
        }
    }

    public List<Map<String, Object>> select(Condition condition) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            if (condition == null || condition.evaluate(row)) {
                result.add(new HashMap<>(row));
            }
        }

        return result;
    }
}