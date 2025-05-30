package com.sumit.rdbms.repository;

import com.sumit.rdbms.Models.Transaction;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class TransactionRepository {
    private final Map<String, Transaction> transactions = new ConcurrentHashMap<>();

    public void saveTransaction(Transaction transaction) {
        transactions.put(transaction.getId(), transaction);
    }

    public Transaction getTransaction(String transactionId) {
        return transactions.get(transactionId);
    }

    public List<Transaction> getAllTransactions() {
        return new ArrayList<>(transactions.values());
    }

    public void removeTransaction(String transactionId) {
        transactions.remove(transactionId);
    }
}
