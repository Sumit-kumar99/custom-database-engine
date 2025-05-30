package com.sumit.rdbms.Models;

import java.util.Map;

public class EqualCondition implements Condition {

    private static final long serialVersionUID = 1L;

    private String column;
    private Object value;

    public EqualCondition() {}

    public EqualCondition(String column, Object value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public boolean evaluate(Map<String, Object> row) {
        Object rowValue = row.get(column);
        if (rowValue == null) {
            return value == null;
        }
        return rowValue.equals(value);
    }

    // Getters and setters
    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
