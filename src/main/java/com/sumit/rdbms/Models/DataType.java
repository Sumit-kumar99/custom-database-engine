package com.sumit.rdbms.Models;

public enum DataType {
    STRING, INTEGER, DOUBLE, BOOLEAN, DATE;

    public boolean isCompatible(Object value) {
        if (value == null) {
            return true;
        }

        switch (this) {
            case STRING:
                return value instanceof String;
            case INTEGER:
                return value instanceof Integer;
            case DOUBLE:
                return value instanceof Double || value instanceof Integer;
            default:
                return false;
        }
    }
}
