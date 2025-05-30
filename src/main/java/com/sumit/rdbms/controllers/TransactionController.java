package com.sumit.rdbms.controllers;

import com.sumit.rdbms.Models.EqualCondition;
import com.sumit.rdbms.Models.Transaction;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {
    @Autowired
    private TransactionService transactionService;

    @PostMapping("/begin")
    public ResponseEntity<?> beginTransaction() {
        try {
            Transaction tx = transactionService.beginTransaction();
            return ResponseEntity.ok().body(Map.of(
                    "message", "Transaction started",
                    "transactionId", tx.getId()
            ));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{transactionId}/commit")
    public ResponseEntity<?> commitTransaction(@PathVariable String transactionId) {
        try {
            transactionService.commit(transactionId);
            return ResponseEntity.ok().body(Map.of("message", "Transaction committed"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{transactionId}/abort")
    public ResponseEntity<?> abortTransaction(@PathVariable String transactionId) {
        try {
            transactionService.abort(transactionId);
            return ResponseEntity.ok().body(Map.of("message", "Transaction aborted"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{transactionId}/tables/{tableName}/rows")
    public ResponseEntity<?> insertInTransaction(
            @PathVariable String transactionId,
            @PathVariable String tableName,
            @RequestBody Map<String, Object> values) {
        try {
            transactionService.insertInTransaction(transactionId, tableName, values);
            return ResponseEntity.ok().body(Map.of("message", "Operation added to transaction"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{transactionId}/tables/{tableName}/rows")
    public ResponseEntity<?> selectInTransaction(
            @PathVariable String transactionId,
            @PathVariable String tableName,
            @RequestBody(required = false) EqualCondition condition) {
        try {
            List<Map<String, Object>> results =
                    transactionService.selectInTransaction(transactionId, tableName, condition);
            return ResponseEntity.ok().body(results);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
