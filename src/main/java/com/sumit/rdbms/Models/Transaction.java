package com.sumit.rdbms.Models;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private TransactionStatus status;
    private final Date startTime;
    private Date commitTime;
    private Map<String, List<TransactionOperation>> operations;
    private static final AtomicLong idGenerator = new AtomicLong(0);

    public Transaction() {
        this.id = "tx-" + idGenerator.incrementAndGet();
        this.status = TransactionStatus.ACTIVE;
        this.startTime = new Date();
        this.operations = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
        if (status == TransactionStatus.COMMITTED) {
            this.commitTime = new Date();
        }
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getCommitTime() {
        return commitTime;
    }

    public void addOperation(String tableName, TransactionOperation operation) {
        if (!operations.containsKey(tableName)) {
            operations.put(tableName, new ArrayList<>());
        }
        operations.get(tableName).add(operation);
    }

    public Map<String, List<TransactionOperation>> getOperations() {
        return operations;
    }
}
