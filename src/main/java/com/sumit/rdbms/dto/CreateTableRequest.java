package com.sumit.rdbms.dto;

import com.sumit.rdbms.Models.Column;

import java.util.*;

public class CreateTableRequest {
    private String tableName;
    private List<Column> columns;

    // Getters and setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
