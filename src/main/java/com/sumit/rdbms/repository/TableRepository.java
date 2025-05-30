package com.sumit.rdbms.repository;


import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class TableRepository {
    @Autowired
    private StorageService storageService;

    public void saveTable(Table table) {
        storageService.saveTable(table);
    }

    public Table getTable(String tableName) {
        return storageService.loadTable(tableName);
    }

    public void deleteTable(String tableName) {
        storageService.deleteTable(tableName);
    }

    public boolean exists(String tableName) {
        return storageService.tableExists(tableName);
    }

    public List<String> listTables() {
        return storageService.listTables();
    }
}
