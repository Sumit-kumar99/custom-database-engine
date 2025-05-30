package com.sumit.rdbms.service;

import com.sumit.rdbms.util.SQLParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryService {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private SQLParser sqlParser;

    public Object executeQuery(String query) {
        return sqlParser.parseAndExecute(query, databaseService);
    }
}
