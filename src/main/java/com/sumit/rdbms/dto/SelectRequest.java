package com.sumit.rdbms.dto;

import com.sumit.rdbms.Models.Condition;

public class SelectRequest {
    private String tableName;
    private Condition condition;

    // Getters and setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }
}
