package com.sumit.rdbms.controllers;

import com.sumit.rdbms.Models.EqualCondition;
import com.sumit.rdbms.Models.JoinResult;
import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.dto.CreateTableRequest;
import com.sumit.rdbms.dto.JoinRequest;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tables")
public class TableController {

    @Autowired
    private DatabaseService databaseService;

    @PostMapping
    public ResponseEntity<?> createTable(@RequestBody CreateTableRequest request) {
        try {
            databaseService.createTable(request.getTableName(), request.getColumns());
            return ResponseEntity.ok().body(Map.of("message", "Table created successfully"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{tableName}")
    public ResponseEntity<?> dropTable(@PathVariable String tableName) {
        try {
            databaseService.dropTable(tableName);
            return ResponseEntity.ok().body(Map.of("message", "Table dropped successfully"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{tableName}/rows")
    public ResponseEntity<?> insertData(@PathVariable String tableName,
                                        @RequestBody Map<String, Object> values) {
        try {
            databaseService.insert(tableName, values);
            return ResponseEntity.ok().body(Map.of("message", "Data inserted successfully"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{tableName}/rows")
    public ResponseEntity<?> selectData(@PathVariable String tableName,
                                        @RequestBody(required = false) EqualCondition condition) {
        try {
            List<Map<String, Object>> results = databaseService.select(tableName, condition);
            return ResponseEntity.ok().body(results);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> listTables() {
        try {
            List<String> tables = databaseService.listTables();
            return ResponseEntity.ok().body(tables);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<?> getTableSchema(@PathVariable String tableName) {
        try {
            Table table = databaseService.getTable(tableName);
            return ResponseEntity.ok().body(table);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // New JOIN endpoint
    @PostMapping("/join")
    public ResponseEntity<?> performJoin(@RequestBody JoinRequest request) {
        try {
            JoinResult result = databaseService.performJoin(
                    request.getLeftTableName(),
                    request.getRightTableName(),
                    request.getJoinType(),
                    request.getJoinCondition()
            );

            return ResponseEntity.ok().body(Map.of(
                    "columns", result.getColumns(),
                    "rows", result.getRows(),
                    "leftTable", result.getLeftTableName(),
                    "rightTable", result.getRightTableName(),
                    "rowCount", result.getRows().size()
            ));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    // Hash JOIN endpoint for better performance
    @PostMapping("/hash-join")
    public ResponseEntity<?> performHashJoin(@RequestBody JoinRequest request) {
        try {
            JoinResult result = databaseService.performHashJoin(
                    request.getLeftTableName(),
                    request.getRightTableName(),
                    request.getJoinType(),
                    request.getJoinCondition()
            );

            return ResponseEntity.ok().body(Map.of(
                    "columns", result.getColumns(),
                    "rows", result.getRows(),
                    "leftTable", result.getLeftTableName(),
                    "rightTable", result.getRightTableName(),
                    "rowCount", result.getRows().size(),
                    "joinMethod", "hash"
            ));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
