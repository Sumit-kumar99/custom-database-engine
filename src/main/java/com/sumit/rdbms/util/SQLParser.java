package com.sumit.rdbms.util;

//import Models.*;
//import com.sumit.rdbms.Models.*;
import com.sumit.rdbms.Models.*;
import com.sumit.rdbms.exception.DatabaseException;
import com.sumit.rdbms.service.DatabaseService;
import com.sumit.rdbms.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

@Component
public class SQLParser {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private IndexService indexService;

    // Add this method to handle CREATE INDEX
    private Object handleCreateIndex(String query) {
        // Parse CREATE INDEX statement
        // Format: CREATE INDEX indexName ON tableName (columnName)
        int indexNameStart = "CREATE INDEX".length();
        int onIndex = query.toUpperCase().indexOf(" ON ");

        if (onIndex == -1) {
            throw new DatabaseException("Invalid CREATE INDEX statement");
        }

        String indexName = query.substring(indexNameStart, onIndex).trim();
        int columnStart = query.indexOf('(', onIndex);
        int columnEnd = query.indexOf(')', columnStart);

        if (columnStart == -1 || columnEnd == -1) {
            throw new DatabaseException("Invalid column specification in CREATE INDEX");
        }

        String tableName = query.substring(onIndex + 4, columnStart).trim();
        String columnName = query.substring(columnStart + 1, columnEnd).trim();

        // Default to BTREE index type
        IndexType indexType = IndexType.BTREE;

        // Check if USING clause is present
        int usingIndex = query.toUpperCase().indexOf("USING");
        if (usingIndex != -1 && usingIndex > columnEnd) {
            String typeStr = query.substring(usingIndex + 5).trim().toUpperCase();
            if (typeStr.contains("HASH")) {
                indexType = IndexType.HASH;
            }
        }

        indexService.createIndex(tableName, indexName, columnName, indexType);
        return Map.of("message", "Index created: " + indexName);
    }

    // Modify parseAndExecute to handle CREATE INDEX
    public Object parseAndExecute(String query) {
        String[] tokens = query.split("\\s+");

        if (tokens.length < 2) {
            throw new DatabaseException("Invalid query");
        }

        String command = tokens[0].toUpperCase();
        String subCommand = tokens.length > 1 ? tokens[1].toUpperCase() : "";

        if (command.equals("CREATE") && subCommand.equals("INDEX")) {
            return handleCreateIndex(query);
        }

        // Rest of the method remains the same...
        // Handle other commands: CREATE TABLE, DROP, INSERT, SELECT

        return null; // Placeholder for brevity
    }




    public Object parseAndExecute(String query, DatabaseService databaseService) {
        String[] tokens = query.split("\\s+");

        if (tokens.length == 0) {
            throw new DatabaseException("Empty query");
        }

        String command = tokens[0].toUpperCase();

        switch (command) {
            case "CREATE":
                return handleCreate(query, databaseService);
            case "DROP":
                return handleDrop(query, databaseService);
            case "INSERT":
                return handleInsert(query, databaseService);
            case "SELECT":
                return handleSelect(query, databaseService);
            default:
                throw new DatabaseException("Unknown command: " + command);
        }
    }

    private Object handleCreate(String query, DatabaseService databaseService) {
        // Parse CREATE TABLE statement
        int tableIndex = query.toUpperCase().indexOf("TABLE");
        if (tableIndex == -1) {
            throw new DatabaseException("Invalid CREATE statement");
        }

        int nameStart = tableIndex + 5;
        int parensStart = query.indexOf('(', nameStart);
        if (parensStart == -1) {
            throw new DatabaseException("Missing column definitions");
        }

        String tableName = query.substring(nameStart, parensStart).trim();
        String columnDefs = query.substring(parensStart + 1, query.lastIndexOf(')'));

        List<Column> columns = new ArrayList<>();
        String[] colDefs = columnDefs.split(",");

        for (String colDef : colDefs) {
            String[] parts = colDef.trim().split("\\s+");
            if (parts.length < 2) {
                throw new DatabaseException("Invalid column definition: " + colDef);
            }

            String colName = parts[0];
            String typeName = parts[1].toUpperCase();
            boolean required = colDef.toUpperCase().contains("NOT NULL");

            DataType dataType;
            switch (typeName) {
                case "VARCHAR":
                case "TEXT":
                case "STRING":
                    dataType = DataType.STRING;
                    break;
                case "INT":
                case "INTEGER":
                    dataType = DataType.INTEGER;
                    break;
                case "DOUBLE":
                case "FLOAT":
                    dataType = DataType.DOUBLE;
                    break;
                default:
                    throw new DatabaseException("Unsupported data type: " + typeName);
            }

            columns.add(new Column(colName, dataType, required));
        }

        databaseService.createTable(tableName, columns);
        return Map.of("message", "Table created: " + tableName);
    }

    private Object handleDrop(String query, DatabaseService databaseService) {
        // Parse DROP TABLE statement
        int tableIndex = query.toUpperCase().indexOf("TABLE");
        if (tableIndex == -1) {
            throw new DatabaseException("Invalid DROP statement");
        }

        String tableName = query.substring(tableIndex + 5).trim();
        databaseService.dropTable(tableName);
        return Map.of("message", "Table dropped: " + tableName);
    }

    private Object handleInsert(String query, DatabaseService databaseService) {
        // Parse INSERT INTO statement
        int intoIndex = query.toUpperCase().indexOf("INTO");
        if (intoIndex == -1) {
            throw new DatabaseException("Invalid INSERT statement");
        }

        int valuesIndex = query.toUpperCase().indexOf("VALUES");
        if (valuesIndex == -1) {
            throw new DatabaseException("Missing VALUES in INSERT statement");
        }

        String tableName = query.substring(intoIndex + 4, valuesIndex).trim();
        String valuesList = query.substring(query.indexOf('(', valuesIndex) + 1, query.lastIndexOf(')'));

        // Get table schema
        Table table = databaseService.getTable(tableName);
        List<Column> columns = table.getColumns();
        String[] values = valuesList.split(",");

        if (values.length != columns.size()) {
            throw new DatabaseException("Column count doesn't match value count");
        }

        Map<String, Object> rowValues = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String value = values[i].trim();
            Column column = columns.get(i);

            // Convert string value to appropriate type
            Object typedValue;
            if (value.equals("NULL") || value.equals("null")) {
                typedValue = null;
            } else {
                switch (column.getDataType()) {
                    case STRING:
                        // Remove quotes
                        if ((value.startsWith("'") && value.endsWith("'")) ||
                                (value.startsWith("\"") && value.endsWith("\""))) {
                            typedValue = value.substring(1, value.length() - 1);
                        } else {
                            typedValue = value;
                        }
                        break;
                    case INTEGER:
                        typedValue = Integer.parseInt(value);
                        break;
                    case DOUBLE:
                        typedValue = Double.parseDouble(value);
                        break;
                    default:
                        throw new DatabaseException("Unsupported data type");
                }
            }

            rowValues.put(column.getName(), typedValue);
        }

        databaseService.insert(tableName, rowValues);
        return Map.of("message", "1 row inserted");
    }

    private Object handleSelect(String query, DatabaseService databaseService) {
        // Parse SELECT statement
        int fromIndex = query.toUpperCase().indexOf("FROM");
        if (fromIndex == -1) {
            throw new DatabaseException("Invalid SELECT statement");
        }

        String tableName;
        Condition condition = null;

        int whereIndex = query.toUpperCase().indexOf("WHERE");
        if (whereIndex == -1) {
            tableName = query.substring(fromIndex + 4).trim();
        } else {
            tableName = query.substring(fromIndex + 4, whereIndex).trim();
            String whereClause = query.substring(whereIndex + 5).trim();

            // Simple equality condition parsing
            if (whereClause.contains("=")) {
                String[] parts = whereClause.split("=");
                if (parts.length != 2) {
                    throw new DatabaseException("Invalid WHERE clause");
                }

                String column = parts[0].trim();
                String value = parts[1].trim();

                // Parse value based on format
                Object typedValue;
                if (value.equals("NULL") || value.equals("null")) {
                    typedValue = null;
                } else if ((value.startsWith("'") && value.endsWith("'")) ||
                        (value.startsWith("\"") && value.endsWith("\""))) {
                    typedValue = value.substring(1, value.length() - 1);
                } else if (value.contains(".")) {
                    typedValue = Double.parseDouble(value);
                } else {
                    try {
                        typedValue = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        typedValue = value;
                    }
                }
                condition = new EqualCondition(column, typedValue);
            }
        }

        return databaseService.select(tableName, condition);
    }
}
