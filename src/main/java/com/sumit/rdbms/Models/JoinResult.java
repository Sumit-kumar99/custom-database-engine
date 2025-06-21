package com.sumit.rdbms.Models;

import java.util.List;
import java.util.Map;

public class JoinResult {
    private List<String> columns;
    private List<Map<String, Object>> rows;
    private String leftTableName;
    private String rightTableName;

    public JoinResult(List<String> columns, List<Map<String, Object>> rows,
                      String leftTableName, String rightTableName) {
        this.columns = columns;
        this.rows = rows;
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
    }

    // Getters and setters
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public List<Map<String, Object>> getRows() { return rows; }
    public void setRows(List<Map<String, Object>> rows) { this.rows = rows; }
    public String getLeftTableName() { return leftTableName; }
    public void setLeftTableName(String leftTableName) { this.leftTableName = leftTableName; }
    public String getRightTableName() { return rightTableName; }
    public void setRightTableName(String rightTableName) { this.rightTableName = rightTableName; }
}
