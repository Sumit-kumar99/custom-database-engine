package com.sumit.rdbms.controllers;

import com.sumit.rdbms.Models.TableIndex;
import com.sumit.rdbms.dto.CreateIndexRequest;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/indices")
public class IndexController {
    @Autowired
    private IndexService indexService;

    @PostMapping
    public ResponseEntity<?> createIndex(@RequestBody CreateIndexRequest request) {
        try {
            indexService.createIndex(
                    request.getTableName(),
                    request.getIndexName(),
                    request.getColumnName(),
                    request.getIndexType()
            );
            return ResponseEntity.ok().body(Map.of("message", "Index created successfully"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{tableName}/{indexName}")
    public ResponseEntity<?> dropIndex(@PathVariable String tableName, @PathVariable String indexName) {
        try {
            indexService.dropIndex(tableName, indexName);
            return ResponseEntity.ok().body(Map.of("message", "Index dropped successfully"));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<?> listIndices(@PathVariable String tableName) {
        try {
            List<TableIndex> indices = indexService.listIndices(tableName);
            return ResponseEntity.ok().body(indices);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
