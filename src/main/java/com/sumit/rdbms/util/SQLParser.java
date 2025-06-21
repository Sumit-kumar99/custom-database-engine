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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SQLParser {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private IndexService indexService;

    // Pattern for JOIN queries
    private static final Pattern JOIN_PATTERN = Pattern.compile(
            "SELECT\\s+\\*\\s+FROM\\s+(\\w+)\\s+" +
                    "(INNER\\s+JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|FULL\\s+OUTER\\s+JOIN)\\s+" +
                    "(\\w+)\\s+ON\\s+(\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+)(?:\\s+WHERE\\s+(.+))?",
            Pattern.CASE_INSENSITIVE
    );

    // Add this method to handle CREATE INDEX
//    private Object handleCreateIndex(String query) {
//        // Parse CREATE INDEX statement
//        // Format: CREATE INDEX indexName ON tableName (columnName)
//        int indexNameStart = "CREATE INDEX".length();
//        int onIndex = query.toUpperCase().indexOf(" ON ");
//
//        if (onIndex == -1) {
//            throw new DatabaseException("Invalid CREATE INDEX statement");
//        }
//
//        String indexName = query.substring(indexNameStart, onIndex).trim();
//        int columnStart = query.indexOf('(', onIndex);
//        int columnEnd = query.indexOf(')', columnStart);
//
//        if (columnStart == -1 || columnEnd == -1) {
//            throw new DatabaseException("Invalid column specification in CREATE INDEX");
//        }
//
//        String tableName = query.substring(onIndex + 4, columnStart).trim();
//        String columnName = query.substring(columnStart + 1, columnEnd).trim();
//
//        // Default to BTREE index type
//        IndexType indexType = IndexType.BTREE;
//
//        // Check if USING clause is present
//        int usingIndex = query.toUpperCase().indexOf("USING");
//        if (usingIndex != -1 && usingIndex > columnEnd) {
//            String typeStr = query.substring(usingIndex + 5).trim().toUpperCase();
//            if (typeStr.contains("HASH")) {
//                indexType = IndexType.HASH;
//            }
//        }
//
//        indexService.createIndex(tableName, indexName, columnName, indexType);
//        return Map.of("message", "Index created: " + indexName);
//    }

    // Modify parseAndExecute to handle CREATE INDEX
//    public Object parseAndExecute(String query) {
//        String[] tokens = query.split("\\s+");
//
//        if (tokens.length < 2) {
//            throw new DatabaseException("Invalid query");
//        }
//
//        String command = tokens[0].toUpperCase();
//        String subCommand = tokens.length > 1 ? tokens[1].toUpperCase() : "";
//
//        if (command.equals("CREATE") && subCommand.equals("INDEX")) {
//            return handleCreateIndex(query);
//        }
//
//        // Rest of the method remains the same...
//        // Handle other commands: CREATE TABLE, DROP, INSERT, SELECT
//
//        return null; // Placeholder for brevity
//    }




//    public Object parseAndExecute(String query, DatabaseService databaseService) {
//        String[] tokens = query.split("\\s+");
//
//        if (tokens.length == 0) {
//            throw new DatabaseException("Empty query");
//        }
//
//        String command = tokens[0].toUpperCase();
//
//        switch (command) {
//            case "CREATE":
//                return handleCreate(query, databaseService);
//            case "DROP":
//                return handleDrop(query, databaseService);
//            case "INSERT":
//                return handleInsert(query, databaseService);
//            case "SELECT":
//                return handleSelect(query, databaseService);
//            default:
//                throw new DatabaseException("Unknown command: " + command);
//        }
//    }

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

//    private Object handleSelect(String query, DatabaseService databaseService) {
//        /*
//         * STEP 1: PARSE THE SELECT STATEMENT
//         * ================================
//         * This section extracts the table name and WHERE conditions from the SQL query
//         */
//
//        // Find the FROM keyword - every SELECT must have FROM
//        int fromIndex = query.toUpperCase().indexOf("FROM");
//        if (fromIndex == -1) {
//            throw new DatabaseException("Invalid SELECT statement");
//        }
//
//        String tableName;
//        Condition condition = null;
//
//        /*
//         * STEP 2: DETERMINE IF THERE'S A WHERE CLAUSE
//         * ==========================================
//         * Two scenarios:
//         * 1. SELECT * FROM users          (no WHERE clause)
//         * 2. SELECT * FROM users WHERE id = 5   (has WHERE clause)
//         */
//
//        int whereIndex = query.toUpperCase().indexOf("WHERE");
//        if (whereIndex == -1) {
//            // No WHERE clause - select all rows
//            // Example: "SELECT * FROM users"
//            tableName = query.substring(fromIndex + 4).trim();
//            // condition remains null
//        } else {
//            // Has WHERE clause - need to parse conditions
//            // Example: "SELECT * FROM users WHERE email = 'john@example.com'"
//
//            // Extract table name (between FROM and WHERE)
//            tableName = query.substring(fromIndex + 4, whereIndex).trim();
//
//            // Extract the condition part (after WHERE)
//            String whereClause = query.substring(whereIndex + 5).trim();
//
//            // Parse the WHERE clause into a Condition object
//            condition = parseWhereClause(whereClause);
//        }
//
//        /*
//         * STEP 3: VALIDATE TABLE EXISTS
//         * =============================
//         * Before proceeding, ensure the table actually exists
//         */
//
//        Table table = databaseService.getTable(tableName);
//        if (table == null) {
//            throw new DatabaseException("Table does not exist: " + tableName);
//        }
//
//        /*
//         * STEP 4: INDEX OPTIMIZATION LOGIC
//         * ===============================
//         * This is where the magic happens - decide whether to use indexes or not
//         */
//
//        List<Map<String, Object>> results = executeSelectWithIndexOptimization(table, condition);
//
//        /*
//         * STEP 5: RETURN FORMATTED RESULTS
//         * ===============================
//         * Return results in a structured format with metadata
//         */
//
//        return Map.of(
//                "rows", results,                    // The actual data rows
//                "count", results.size(),            // Number of rows returned
//                "message", results.size() + " rows selected"  // Status message
//        );
//    }
//
//    /*
//     * INDEX OPTIMIZATION ENGINE
//     * ========================
//     * This method determines the best way to execute the query
//     */
//    private List<Map<String, Object>> executeSelectWithIndexOptimization(Table table, Condition condition) {
//
//        /*
//         * SCENARIO 1: NO WHERE CLAUSE
//         * ===========================
//         * If there's no condition, we need ALL rows
//         * No index can help here - must scan entire table
//         */
//        if (condition == null) {
//            System.out.println("No WHERE clause - performing full table scan");
//            return table.select(null);  // Returns all rows
//        }
//
//        /*
//         * SCENARIO 2: EQUALITY CONDITION (Perfect for Index Usage)
//         * ========================================================
//         * Example: WHERE email = 'john@example.com'
//         * This is ideal for index usage because:
//         * - Hash indexes: O(1) lookup
//         * - B-tree indexes: O(log n) lookup
//         */
//        if (condition instanceof EqualCondition) {
//            EqualCondition eqCondition = (EqualCondition) condition;
//
//            /*
//             * INDEX SEARCH ALGORITHM
//             * =====================
//             * Look through all available indexes on this table
//             * to find one that matches the column we're querying
//             */
//            for (TableIndex index : table.getIndices()) {
//                if (index.getColumnName().equals(eqCondition.getColumn())) {
//
//                    // FOUND A MATCHING INDEX!
//                    System.out.println("🚀 INDEX HIT! Using index: " + index.getName() +
//                            " (type: " + index.getIndexType() + ")" +
//                            " for column: " + eqCondition.getColumn());
//
//                    /*
//                     * INDEX LOOKUP PROCESS
//                     * ===================
//                     * 1. Use index to find row positions quickly
//                     * 2. Retrieve actual row data from those positions
//                     *
//                     * Performance benefit:
//                     * - Without index: O(n) - scan every row
//                     * - With index: O(1) for hash, O(log n) for btree
//                     */
//                    return table.selectWithIndex(eqCondition);
//                }
//            }
//
//            // No suitable index found for this column
//            System.out.println("⚠️  INDEX MISS! No suitable index found for column: " +
//                    eqCondition.getColumn() + ". Performing full table scan.");
//            System.out.println("💡 Suggestion: CREATE INDEX idx_" + eqCondition.getColumn() +
//                    " ON " + table.getName() + " (" + eqCondition.getColumn() + ")");
//        }
//
//        /*
//         * SCENARIO 3: COMPLEX CONDITIONS
//         * ==============================
//         * Examples:
//         * - WHERE age > 25
//         * - WHERE name != 'John'
//         * - WHERE salary BETWEEN 50000 AND 100000
//         *
//         * Current implementation doesn't optimize these (but could be enhanced)
//         */
//        else {
//            System.out.println("Complex condition detected - using full table scan");
//            System.out.println("Condition type: " + condition.getClass().getSimpleName());
//        }
//
//        /*
//         * FALLBACK: FULL TABLE SCAN
//         * =========================
//         * When no index can be used, scan every row
//         * This is the "brute force" approach but always works
//         */
//        return table.select(condition);
//    }

    /*
     * WHERE CLAUSE PARSING ENGINE
     * ===========================
     * Converts SQL WHERE conditions into Java Condition objects
     */
//    private Condition parseWhereClause(String whereClause) {
//        /*
//         * PARSING STRATEGY
//         * ===============
//         * Check for different operators in order of complexity:
//         * 1. Equality (=) - most common, best for indexes
//         * 2. Inequality (!=, <>) - less common
//         * 3. Comparisons (>, <, >=, <=) - range queries
//         */
//
//        if (whereClause.contains("=")) {
//            // Example: "email = 'john@example.com'"
//            return parseEqualCondition(whereClause, "=");
//
//        }
////        else if (whereClause.contains("!=") || whereClause.contains("<>")) {
////            // Example: "status != 'inactive'"
////            // Note: These typically can't use indexes effectively
////            return parseNotEqualCondition(whereClause);
////
////        } else if (whereClause.contains(">=")) {
////            // Example: "age >= 18"
////            return parseComparisonCondition(whereClause, ">=");
////
////        } else if (whereClause.contains("<=")) {
////            // Example: "salary <= 100000"
////            return parseComparisonCondition(whereClause, "<=");
////
////        } else if (whereClause.contains(">")) {
////            // Example: "score > 80"
////            return parseComparisonCondition(whereClause, ">");
////
////        } else if (whereClause.contains("<")) {
////            // Example: "age < 65"
////            return parseComparisonCondition(whereClause, "<");
////        }
//
//        throw new DatabaseException("Unsupported WHERE clause: " + whereClause);
//    }
//
//    /*
//     * EQUALITY CONDITION PARSER
//     * ========================
//     * Handles: column = value
//     * This is the BEST condition type for index usage!
//     */
//    private EqualCondition parseEqualCondition(String whereClause, String operator) {
//        /*
//         * PARSING STEPS:
//         * 1. Split by '=' operator
//         * 2. Extract column name (left side)
//         * 3. Extract and convert value (right side)
//         */
//
//        String[] parts = whereClause.split(operator, 2);
//        if (parts.length != 2) {
//            throw new DatabaseException("Invalid WHERE clause");
//        }
//
//        String column = parts[0].trim();        // e.g., "email"
//        String value = parts[1].trim();         // e.g., "'john@example.com'"
//        Object typedValue = parseValue(value);  // Convert to proper type
//
//        /*
//         * WHY EqualCondition IS PERFECT FOR INDEXES:
//         * =========================================
//         * - Hash Index: Direct lookup in hash table - O(1)
//         * - B-tree Index: Binary search in sorted tree - O(log n)
//         * - Both are much faster than scanning all rows - O(n)
//         */
//
//        return new EqualCondition(column, typedValue);
//    }
//
//    /*
//     * VALUE TYPE CONVERSION
//     * ====================
//     * Converts string values from SQL into proper Java types
//     */
//    private Object parseValue(String value) {
//        /*
//         * TYPE DETECTION LOGIC:
//         * 1. NULL values
//         * 2. Quoted strings ('text' or "text")
//         * 3. Decimal numbers (contain '.')
//         * 4. Integers
//         * 5. Unquoted strings (fallback)
//         */
//
//        if (value.equals("NULL") || value.equals("null")) {
//            return null;
//
//        } else if ((value.startsWith("'") && value.endsWith("'")) ||
//                (value.startsWith("\"") && value.endsWith("\""))) {
//            // Remove quotes: 'john@example.com' -> john@example.com
//            return value.substring(1, value.length() - 1);
//
//        } else if (value.contains(".")) {
//            // Try to parse as decimal: 123.45 -> 123.45
//            try {
//                return Double.parseDouble(value);
//            } catch (NumberFormatException e) {
//                return value; // Keep as string if not a valid number
//            }
//
//        } else {
//            // Try to parse as integer: 123 -> 123
//            try {
//                return Integer.parseInt(value);
//            } catch (NumberFormatException e) {
//                return value; // Keep as string if not a valid number
//            }
//        }
//    }
//
//    /*
//     * PERFORMANCE COMPARISON EXAMPLE
//     * =============================
//     *
//     * Scenario: Table with 1,000,000 users, query: SELECT * FROM users WHERE email = 'john@example.com'
//     *
//     * WITHOUT INDEX (Full Table Scan):
//     * - Must check every single row: 1,000,000 comparisons
//     * - Time complexity: O(n) = O(1,000,000)
//     * - Estimated time: ~100ms (depending on hardware)
//     *
//     * WITH HASH INDEX:
//     * - Direct hash table lookup: 1 comparison
//     * - Time complexity: O(1)
//     * - Estimated time: ~0.1ms
//     * - Speed improvement: 1000x faster!
//     *
//     * WITH BTREE INDEX:
//     * - Binary search in sorted tree: ~20 comparisons (log₂(1,000,000) ≈ 20)
//     * - Time complexity: O(log n) = O(20)
//     * - Estimated time: ~1ms
//     * - Speed improvement: 100x faster!
//     */
//
//    /*
//     * INDEX SELECTION STRATEGY
//     * =======================
//     *
//     * The algorithm chooses the FIRST matching index it finds.
//     * In a production system, you might want to:
//     *
//     * 1. Prefer more selective indexes (fewer duplicate values)
//     * 2. Consider index type (hash vs btree) based on query type
//     * 3. Use cost-based optimization to choose the best index
//     *
//     * Current implementation is simple but effective for most cases.
//     */


    private Object handleJoinQuery(Matcher matcher) {
        String leftTable = matcher.group(1);
        String joinTypeStr = matcher.group(2).toUpperCase().replaceAll("\\s+", "_");
        String rightTable = matcher.group(3);
        String leftColumn = matcher.group(4);
        String rightColumn = matcher.group(5);
        String whereClause = matcher.group(6);

        // Parse join type
        JoinType joinType;
        switch (joinTypeStr) {
            case "INNER_JOIN":
                joinType = JoinType.INNER;
                break;
            case "LEFT_JOIN":
                joinType = JoinType.LEFT;
                break;
            case "RIGHT_JOIN":
                joinType = JoinType.RIGHT;
                break;
            case "FULL_OUTER_JOIN":
                joinType = JoinType.FULL_OUTER;
                break;
            default:
                throw new DatabaseException("Unsupported join type: " + joinTypeStr);
        }

        // Parse column names (remove table prefixes)
        String leftColName = leftColumn.substring(leftColumn.indexOf('.') + 1);
        String rightColName = rightColumn.substring(rightColumn.indexOf('.') + 1);

        // Create join condition
        JoinCondition joinCondition = new EqualityJoinCondition(leftColName, rightColName);

        // Execute join
        JoinResult result = databaseService.performHashJoin(leftTable, rightTable, joinType, joinCondition);

        // Apply WHERE clause if present
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            result = applyWhereClauseToJoinResult(result, whereClause.trim());
        }

        return result.getRows();
    }

    private JoinResult applyWhereClauseToJoinResult(JoinResult joinResult, String whereClause) {
        // Simple WHERE clause implementation for joined results
        // This would need to be expanded for complex conditions

        List<Map<String, Object>> filteredRows = new ArrayList<>();

        // Parse simple equality condition
        if (whereClause.contains("=")) {
            String[] parts = whereClause.split("=", 2);
            String column = parts[0].trim();
            String valueStr = parts[1].trim();
            Object value = parseValue(valueStr);

            for (Map<String, Object> row : joinResult.getRows()) {
                Object rowValue = row.get(column);
                if (Objects.equals(rowValue, value)) {
                    filteredRows.add(row);
                }
            }
        } else {
            // For now, return all rows if WHERE clause is not simple equality
            filteredRows = joinResult.getRows();
        }

        return new JoinResult(joinResult.getColumns(), filteredRows,
                joinResult.getLeftTableName(), joinResult.getRightTableName());
    }

    public Object parseAndExecute(String query, DatabaseService databaseService) {

        String trimmedQuery = query.trim();

        // Check if it's a JOIN query
        Matcher joinMatcher = JOIN_PATTERN.matcher(trimmedQuery);
        if (joinMatcher.matches()) {
            return handleJoinQuery(joinMatcher);
        }

        // Handle regular queries
        String[] tokens = trimmedQuery.split("\\s+");

        if (tokens.length == 0) {
            throw new DatabaseException("Empty query");
        }
//        String[] tokens = query.trim().split("\\s+");

        if (tokens.length < 2) {
            throw new DatabaseException("Invalid query");
        }

        String command = tokens[0].toUpperCase();
        String subCommand = tokens.length > 1 ? tokens[1].toUpperCase() : "";

        switch (command) {
            case "CREATE":
                if (subCommand.equals("INDEX")) {
                    return handleCreateIndex(query);
                } else if (subCommand.equals("TABLE")) {
                    return handleCreate(query, databaseService);
                }
                break;
            case "DROP":
                if (subCommand.equals("INDEX")) {
                    return handleDropIndex(query);
                } else if (subCommand.equals("TABLE")) {
                    return handleDrop(query, databaseService);
                }
                break;
            case "INSERT":
                return handleInsert(query, databaseService);
            case "SELECT":
                return handleSelect(query);
            default:
                throw new DatabaseException("Unknown command: " + command);
        }

        throw new DatabaseException("Invalid query format");
    }

    private Object handleCreateIndex(String query) {
        // Parse CREATE INDEX statement
        // Format: CREATE INDEX indexName ON tableName (columnName) [USING BTREE|HASH]

        String upperQuery = query.toUpperCase();
        int indexStart = upperQuery.indexOf("INDEX") + 5;
        int onIndex = upperQuery.indexOf(" ON ");

        if (onIndex == -1) {
            throw new DatabaseException("Invalid CREATE INDEX statement");
        }

        String indexName = query.substring(indexStart, onIndex).trim();
        int columnStart = query.indexOf('(', onIndex);
        int columnEnd = query.indexOf(')', columnStart);

        if (columnStart == -1 || columnEnd == -1) {
            throw new DatabaseException("Invalid column specification in CREATE INDEX");
        }

        String tableName = query.substring(onIndex + 4, columnStart).trim();
        String columnName = query.substring(columnStart + 1, columnEnd).trim();

        // Check for USING clause
        IndexType indexType = IndexType.BTREE; // Default to B-Tree
        int usingIndex = upperQuery.indexOf("USING");
        if (usingIndex != -1 && usingIndex > columnEnd) {
            String typeStr = query.substring(usingIndex + 5).trim().toUpperCase();
            if (typeStr.contains("HASH")) {
                indexType = IndexType.HASH;
            } else if (typeStr.contains("BTREE")) {
                indexType = IndexType.BTREE;
            }
        }

        indexService.createIndex(tableName, indexName, columnName, indexType);
        return Map.of("message", "Index created: " + indexName + " (" + indexType + ")");
    }

    private Object handleDropIndex(String query) {
        // Parse DROP INDEX statement
        // Format: DROP INDEX indexName ON tableName

        String upperQuery = query.toUpperCase();
        int indexStart = upperQuery.indexOf("INDEX") + 5;
        int onIndex = upperQuery.indexOf(" ON ");

        if (onIndex == -1) {
            throw new DatabaseException("Invalid DROP INDEX statement");
        }

        String indexName = query.substring(indexStart, onIndex).trim();
        String tableName = query.substring(onIndex + 4).trim();

        indexService.dropIndex(tableName, indexName);
        return Map.of("message", "Index dropped: " + indexName);
    }

    private Object handleSelect(String query) {
        // Enhanced SELECT with range query support
        // Format: SELECT * FROM tableName [WHERE column = value | column BETWEEN min AND max]

        String upperQuery = query.toUpperCase();
        int fromIndex = upperQuery.indexOf("FROM");
        if (fromIndex == -1) {
            throw new DatabaseException("Invalid SELECT statement");
        }

        String tableName;
        Condition condition = null;

        int whereIndex = upperQuery.indexOf("WHERE");
        if (whereIndex == -1) {
            tableName = query.substring(fromIndex + 4).trim();
        } else {
            tableName = query.substring(fromIndex + 4, whereIndex).trim();
            String whereClause = query.substring(whereIndex + 5).trim();
            condition = parseWhereClause(whereClause);
        }

        return databaseService.select(tableName, condition);
    }

    private Condition parseWhereClause(String whereClause) {
        String upperClause = whereClause.toUpperCase();

        // Check for BETWEEN clause (range query)
        if (upperClause.contains(" BETWEEN ") && upperClause.contains(" AND ")) {
            int betweenIndex = upperClause.indexOf(" BETWEEN ");
            int andIndex = upperClause.indexOf(" AND ", betweenIndex);

            String column = whereClause.substring(0, betweenIndex).trim();
            String minValueStr = whereClause.substring(betweenIndex + 9, andIndex).trim();
            String maxValueStr = whereClause.substring(andIndex + 5).trim();

            Object minValue = parseValue(minValueStr);
            Object maxValue = parseValue(maxValueStr);

            return new RangeCondition(column, minValue, maxValue, true, true);
        }

        // Check for equality condition
        if (whereClause.contains("=")) {
            String[] parts = whereClause.split("=", 2);
            if (parts.length != 2) {
                throw new DatabaseException("Invalid WHERE clause");
            }

            String column = parts[0].trim();
            String valueStr = parts[1].trim();
            Object value = parseValue(valueStr);

            return new EqualCondition(column, value);
        }

        throw new DatabaseException("Unsupported WHERE clause: " + whereClause);
    }

    private Object parseValue(String valueStr) {
        valueStr = valueStr.trim();

        if (valueStr.equalsIgnoreCase("NULL")) {
            return null;
        }

        // String value (quoted)
        if ((valueStr.startsWith("'") && valueStr.endsWith("'")) ||
                (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
            return valueStr.substring(1, valueStr.length() - 1);
        }

        // Numeric value
        try {
            if (valueStr.contains(".")) {
                return Double.parseDouble(valueStr);
            } else {
                return Integer.parseInt(valueStr);
            }
        } catch (NumberFormatException e) {
            // Treat as string if not numeric
            return valueStr;
        }
    }

}
