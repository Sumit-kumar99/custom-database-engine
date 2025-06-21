package com.sumit.rdbms.controllers;


import com.sumit.rdbms.dto.SQLRequest;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.DatabaseService;
import com.sumit.rdbms.util.SQLParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/sql")
public class SQLController {

    @Autowired
    private SQLParser sqlParser;

    @Autowired
    private DatabaseService databaseService;

    @PostMapping("/execute")
    public ResponseEntity<?> executeSQL(@RequestBody SQLRequest request) {
        try {
            Object result = sqlParser.parseAndExecute(request.getQuery(), databaseService);
            return ResponseEntity.ok().body(Map.of(
                    "query", request.getQuery(),
                    "result", result
            ));
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", e.getMessage(),
                    "query", request.getQuery()
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", "Internal server error: " + e.getMessage(),
                    "query", request.getQuery()
            ));
        }
    }
}