package com.sumit.rdbms.service;


import com.sumit.rdbms.Models.IndexType;
import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.Models.TableIndex;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.repository.TableRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IndexService {

    @Autowired
    private TableRepository tableRepository;

    public void createIndex(String tableName, String indexName, String columnName, IndexType indexType) {
        Table table = getTable(tableName);
        table.createIndex(indexName, columnName, indexType);
        tableRepository.saveTable(table);
    }

    public void dropIndex(String tableName, String indexName) {
        Table table = getTable(tableName);
        table.dropIndex(indexName);
        tableRepository.saveTable(table);
    }

    public List<TableIndex> listIndices(String tableName) {
        Table table = getTable(tableName);
        return table.getIndices();
    }

    private Table getTable(String tableName) {
        Table table = tableRepository.getTable(tableName);
        if (table == null) {
            throw new DatabaseException("Table does not exist: " + tableName);
        }
        return table;
    }
}
