package com.sumit.rdbms.service;

//import Models.*;
//import com.sumit.rdbms.Models.*;
import com.sumit.rdbms.Models.*;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.repository.TableRepository;
import com.sumit.rdbms.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class TransactionService {
    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    // Simple locking mechanism - in production, use more sophisticated approach
    private final Map<String, ReentrantReadWriteLock> tableLocks = new HashMap<>();

    public Transaction beginTransaction() {
        Transaction tx = new Transaction();
        transactionRepository.saveTransaction(tx);
        return tx;
    }

    public void commit(String transactionId) {
        Transaction tx = getTransaction(transactionId);

        if (tx.getStatus() != TransactionStatus.ACTIVE) {
            throw new DatabaseException("Transaction is not active: " + transactionId);
        }

        // Apply all operations
        try {
            for (Map.Entry<String, List<TransactionOperation>> entry : tx.getOperations().entrySet()) {
                String tableName = entry.getKey();
                Table table = tableRepository.getTable(tableName);

                if (table == null) {
                    throw new DatabaseException("Table does not exist: " + tableName);
                }

                // Acquire write lock
                ReentrantReadWriteLock lock = getTableLock(tableName);
                lock.writeLock().lock();

                try {
                    // Apply operations in order
                    for (TransactionOperation op : entry.getValue()) {
                        switch (op.getType()) {
                            case INSERT:
                                table.addRow(op.getRowData());
                                break;
                            // Implement UPDATE and DELETE logic here
                            default:
                                // READ operations don't modify state
                                break;
                        }
                    }

                    // Save the updated table
                    tableRepository.saveTable(table);
                } finally {
                    lock.writeLock().unlock();
                }
            }

            tx.setStatus(TransactionStatus.COMMITTED);
            transactionRepository.saveTransaction(tx);
        } catch (Exception e) {
            // If anything goes wrong, abort
            abort(transactionId);
            throw new DatabaseException("Transaction failed: " + e.getMessage(), e);
        }
    }

    public void abort(String transactionId) {
        Transaction tx = getTransaction(transactionId);
        tx.setStatus(TransactionStatus.ABORTED);
        transactionRepository.saveTransaction(tx);
    }

    public void insertInTransaction(String transactionId, String tableName, Map<String, Object> values) {
        Transaction tx = getTransaction(transactionId);

        if (tx.getStatus() != TransactionStatus.ACTIVE) {
            throw new DatabaseException("Transaction is not active: " + transactionId);
        }

        // Validate data
        Table table = tableRepository.getTable(tableName);
        if (table == null) {
            throw new DatabaseException("Table does not exist: " + tableName);
        }

        for (Column column : table.getColumns()) {
            if (column.isRequired() && !values.containsKey(column.getName())) {
                throw new DatabaseException("Missing required column: " + column.getName());
            }

            if (values.containsKey(column.getName())) {
                Object value = values.get(column.getName());
                if (value != null && !column.getDataType().isCompatible(value)) {
                    throw new DatabaseException("Invalid data type for column: " + column.getName());
                }
            }
        }

        // Create a copy of the values with only defined columns
        Map<String, Object> row = table.getColumns().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Column::getName,
                        column -> values.getOrDefault(column.getName(), null)
                ));

        TransactionOperation operation = new TransactionOperation(OperationType.INSERT, row);
        tx.addOperation(tableName, operation);
        transactionRepository.saveTransaction(tx);
    }

    public List<Map<String, Object>> selectInTransaction(String transactionId, String tableName, Condition condition) {
        Transaction tx = getTransaction(transactionId);

        if (tx.getStatus() != TransactionStatus.ACTIVE) {
            throw new DatabaseException("Transaction is not active: " + transactionId);
        }

        ReentrantReadWriteLock lock = getTableLock(tableName);
        lock.readLock().lock();

        try {
            Table table = tableRepository.getTable(tableName);
            if (table == null) {
                throw new DatabaseException("Table does not exist: " + tableName);
            }

            TransactionOperation operation = new TransactionOperation(OperationType.SELECT, condition);
            tx.addOperation(tableName, operation);
            transactionRepository.saveTransaction(tx);

            // Use index if available for EqualCondition
            if (condition instanceof EqualCondition) {
                return table.selectWithIndex((EqualCondition) condition);
            } else {
                return table.select(condition);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private Transaction getTransaction(String transactionId) {
        Transaction tx = transactionRepository.getTransaction(transactionId);
        if (tx == null) {
            throw new DatabaseException("Transaction not found: " + transactionId);
        }
        return tx;
    }

    private ReentrantReadWriteLock getTableLock(String tableName) {
        return tableLocks.computeIfAbsent(tableName, k -> new ReentrantReadWriteLock());
    }
}
