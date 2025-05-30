package com.sumit.rdbms.Models;

import java.io.Serializable;
import java.util.Map;

public class TransactionOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    private OperationType type;
    private Map<String, Object> rowData;
    private Condition condition;

    public TransactionOperation(OperationType type, Map<String, Object> rowData) {
        this.type = type;
        this.rowData = rowData;
    }

    public TransactionOperation(OperationType type, Condition condition) {
        this.type = type;
        this.condition = condition;
    }

    public OperationType getType() {
        return type;
    }

    public Map<String, Object> getRowData() {
        return rowData;
    }

    public Condition getCondition() {
        return condition;
    }
}
