package com.sumit.rdbms.controllers;


import com.sumit.rdbms.dto.QueryRequest;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/query")
public class QueryController {
    @Autowired
    private QueryService queryService;

    @PostMapping
    public ResponseEntity<?> executeQuery(@RequestBody QueryRequest request) {
        try {
            Object result = queryService.executeQuery(request.getQuery());
            return ResponseEntity.ok().body(result);
        } catch (DatabaseException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
