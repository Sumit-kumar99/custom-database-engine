package com.sumit.rdbms.service;

import com.sumit.rdbms.Models.Column;
import com.sumit.rdbms.Models.Condition;
import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.repository.TableRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DatabaseService {

    @Autowired
    private TableRepository tableRepository;

    public void createTable(String tableName, List<Column> columns) {
        if (tableRepository.exists(tableName)) {
            throw new DatabaseException("Table already exists: " + tableName);
        }

        Table table = new Table(tableName, columns);
        tableRepository.saveTable(table);
    }

    public void dropTable(String tableName) {
        if (!tableRepository.exists(tableName)) {
            throw new DatabaseException("Table does not exist: " + tableName);
        }

        tableRepository.deleteTable(tableName);
    }

    public void insert(String tableName, Map<String, Object> values) {
        Table table = getTable(tableName);

        // Validate data
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
                .collect(Collectors.toMap(
                        Column::getName,
                        column -> values.getOrDefault(column.getName(), null)
                ));

        table.addRow(row);
        tableRepository.saveTable(table);
    }

    public List<Map<String, Object>> select(String tableName, Condition condition) {
        Table table = getTable(tableName);

        return table.getRows().stream()
                .filter(row -> condition == null || condition.evaluate(row))
                .map(row -> Map.copyOf(row))
                .collect(Collectors.toList());
    }

    public Table getTable(String tableName) {
        Table table = tableRepository.getTable(tableName);
        if (table == null) {
            throw new DatabaseException("Table does not exist: " + tableName);
        }
        return table;
    }

    public List<String> listTables() {
        return tableRepository.listTables();
    }

}
