package com.sumit.rdbms.service;

import com.sumit.rdbms.Models.*;
import com.sumit.rdbms.exception.DatabaseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class JoinService {

    @Autowired
    private DatabaseService databaseService;

    public JoinResult performJoin(String leftTableName, String rightTableName,
                                  JoinType joinType, JoinCondition joinCondition) {
        Table leftTable = databaseService.getTable(leftTableName);
        Table rightTable = databaseService.getTable(rightTableName);

        List<Map<String, Object>> leftRows = leftTable.getRows();
        List<Map<String, Object>> rightRows = rightTable.getRows();

        // Create column list with table prefixes to avoid conflicts
        List<String> resultColumns = createColumnList(leftTable, rightTable);
        List<Map<String, Object>> resultRows = new ArrayList<>();

        switch (joinType) {
            case INNER:
                resultRows = performInnerJoin(leftRows, rightRows, joinCondition,
                        leftTableName, rightTableName);
                break;
            case LEFT:
                resultRows = performLeftJoin(leftRows, rightRows, joinCondition,
                        leftTableName, rightTableName);
                break;
            case RIGHT:
                resultRows = performRightJoin(leftRows, rightRows, joinCondition,
                        leftTableName, rightTableName);
                break;
            case FULL_OUTER:
                resultRows = performFullOuterJoin(leftRows, rightRows, joinCondition,
                        leftTableName, rightTableName);
                break;
            default:
                throw new DatabaseException("Unsupported join type: " + joinType);
        }

        return new JoinResult(resultColumns, resultRows, leftTableName, rightTableName);
    }

    private List<String> createColumnList(Table leftTable, Table rightTable) {
        List<String> columns = new ArrayList<>();

        // Add left table columns with prefix
        for (Column column : leftTable.getColumns()) {
            columns.add(leftTable.getName() + "." + column.getName());
        }

        // Add right table columns with prefix
        for (Column column : rightTable.getColumns()) {
            columns.add(rightTable.getName() + "." + column.getName());
        }

        return columns;
    }

    private List<Map<String, Object>> performInnerJoin(List<Map<String, Object>> leftRows,
                                                       List<Map<String, Object>> rightRows,
                                                       JoinCondition joinCondition,
                                                       String leftTableName,
                                                       String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> leftRow : leftRows) {
            for (Map<String, Object> rightRow : rightRows) {
                if (joinCondition.matches(leftRow, rightRow)) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                }
            }
        }

        return result;
    }

    private List<Map<String, Object>> performLeftJoin(List<Map<String, Object>> leftRows,
                                                      List<Map<String, Object>> rightRows,
                                                      JoinCondition joinCondition,
                                                      String leftTableName,
                                                      String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> leftRow : leftRows) {
            boolean foundMatch = false;

            for (Map<String, Object> rightRow : rightRows) {
                if (joinCondition.matches(leftRow, rightRow)) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                    foundMatch = true;
                }
            }

            // If no match found, add row with null values for right table
            if (!foundMatch) {
                Map<String, Object> joinedRow = createJoinedRowWithNulls(leftRow, null,
                        leftTableName, rightTableName);
                result.add(joinedRow);
            }
        }

        return result;
    }

    private List<Map<String, Object>> performRightJoin(List<Map<String, Object>> leftRows,
                                                       List<Map<String, Object>> rightRows,
                                                       JoinCondition joinCondition,
                                                       String leftTableName,
                                                       String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> rightRow : rightRows) {
            boolean foundMatch = false;

            for (Map<String, Object> leftRow : leftRows) {
                if (joinCondition.matches(leftRow, rightRow)) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                    foundMatch = true;
                }
            }

            // If no match found, add row with null values for left table
            if (!foundMatch) {
                Map<String, Object> joinedRow = createJoinedRowWithNulls(null, rightRow,
                        leftTableName, rightTableName);
                result.add(joinedRow);
            }
        }

        return result;
    }

    private List<Map<String, Object>> performFullOuterJoin(List<Map<String, Object>> leftRows,
                                                           List<Map<String, Object>> rightRows,
                                                           JoinCondition joinCondition,
                                                           String leftTableName,
                                                           String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<Integer> matchedRightIndices = new HashSet<>();

        // First pass: Left join logic
        for (Map<String, Object> leftRow : leftRows) {
            boolean foundMatch = false;

            for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
                Map<String, Object> rightRow = rightRows.get(rightIndex);

                if (joinCondition.matches(leftRow, rightRow)) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                    matchedRightIndices.add(rightIndex);
                    foundMatch = true;
                }
            }

            // If no match found, add row with null values for right table
            if (!foundMatch) {
                Map<String, Object> joinedRow = createJoinedRowWithNulls(leftRow, null,
                        leftTableName, rightTableName);
                result.add(joinedRow);
            }
        }

        // Second pass: Add unmatched right rows
        for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
            if (!matchedRightIndices.contains(rightIndex)) {
                Map<String, Object> rightRow = rightRows.get(rightIndex);
                Map<String, Object> joinedRow = createJoinedRowWithNulls(null, rightRow,
                        leftTableName, rightTableName);
                result.add(joinedRow);
            }
        }

        return result;
    }

    private Map<String, Object> createJoinedRow(Map<String, Object> leftRow,
                                                Map<String, Object> rightRow,
                                                String leftTableName,
                                                String rightTableName) {
        Map<String, Object> joinedRow = new HashMap<>();

        // Add left table columns with prefix
        if (leftRow != null) {
            for (Map.Entry<String, Object> entry : leftRow.entrySet()) {
                joinedRow.put(leftTableName + "." + entry.getKey(), entry.getValue());
            }
        }

        // Add right table columns with prefix
        if (rightRow != null) {
            for (Map.Entry<String, Object> entry : rightRow.entrySet()) {
                joinedRow.put(rightTableName + "." + entry.getKey(), entry.getValue());
            }
        }

        return joinedRow;
    }

    private Map<String, Object> createJoinedRowWithNulls(Map<String, Object> leftRow,
                                                         Map<String, Object> rightRow,
                                                         String leftTableName,
                                                         String rightTableName) {
        Map<String, Object> joinedRow = new HashMap<>();

        // Get table schemas to know all columns
        Table leftTable = leftRow != null ? databaseService.getTable(leftTableName) : null;
        Table rightTable = rightRow != null ? databaseService.getTable(rightTableName) : null;

        // Add left table columns
        if (leftRow != null) {
            for (Map.Entry<String, Object> entry : leftRow.entrySet()) {
                joinedRow.put(leftTableName + "." + entry.getKey(), entry.getValue());
            }
        } else if (leftTable != null) {
            // Add null values for all left table columns
            for (Column column : leftTable.getColumns()) {
                joinedRow.put(leftTableName + "." + column.getName(), null);
            }
        }

        // Add right table columns
        if (rightRow != null) {
            for (Map.Entry<String, Object> entry : rightRow.entrySet()) {
                joinedRow.put(rightTableName + "." + entry.getKey(), entry.getValue());
            }
        } else if (rightTable != null) {
            // Add null values for all right table columns
            for (Column column : rightTable.getColumns()) {
                joinedRow.put(rightTableName + "." + column.getName(), null);
            }
        }

        return joinedRow;
    }

    // Optimized hash join implementation for better performance on large datasets
    public JoinResult performHashJoin(String leftTableName, String rightTableName,
                                      JoinType joinType, JoinCondition joinCondition) {
        if (!(joinCondition instanceof EqualityJoinCondition)) {
            // Fall back to nested loop join for non-equality conditions
            return performJoin(leftTableName, rightTableName, joinType, joinCondition);
        }

        Table leftTable = databaseService.getTable(leftTableName);
        Table rightTable = databaseService.getTable(rightTableName);

        List<Map<String, Object>> leftRows = leftTable.getRows();
        List<Map<String, Object>> rightRows = rightTable.getRows();

        EqualityJoinCondition eqCondition = (EqualityJoinCondition) joinCondition;

        // Build hash map for smaller table (right table in this case)
        Map<Object, List<Map<String, Object>>> rightHashMap = new HashMap<>();
        for (Map<String, Object> rightRow : rightRows) {
            Object key = rightRow.get(eqCondition.getRightColumn());
            rightHashMap.computeIfAbsent(key, k -> new ArrayList<>()).add(rightRow);
        }

        List<String> resultColumns = createColumnList(leftTable, rightTable);
        List<Map<String, Object>> resultRows = new ArrayList<>();

        switch (joinType) {
            case INNER:
                resultRows = performHashInnerJoin(leftRows, rightHashMap, eqCondition,
                        leftTableName, rightTableName);
                break;
            case LEFT:
                resultRows = performHashLeftJoin(leftRows, rightHashMap, eqCondition,
                        leftTableName, rightTableName);
                break;
            // For RIGHT and FULL OUTER joins, hash join optimization is more complex
            // so we fall back to nested loop join
            case RIGHT:
            case FULL_OUTER:
                return performJoin(leftTableName, rightTableName, joinType, joinCondition);
            default:
                throw new DatabaseException("Unsupported join type: " + joinType);
        }

        return new JoinResult(resultColumns, resultRows, leftTableName, rightTableName);
    }

    private List<Map<String, Object>> performHashInnerJoin(List<Map<String, Object>> leftRows,
                                                           Map<Object, List<Map<String, Object>>> rightHashMap,
                                                           EqualityJoinCondition joinCondition,
                                                           String leftTableName,
                                                           String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> leftRow : leftRows) {
            Object leftKey = leftRow.get(joinCondition.getLeftColumn());
            List<Map<String, Object>> matchingRightRows = rightHashMap.get(leftKey);

            if (matchingRightRows != null) {
                for (Map<String, Object> rightRow : matchingRightRows) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                }
            }
        }

        return result;
    }

    private List<Map<String, Object>> performHashLeftJoin(List<Map<String, Object>> leftRows,
                                                          Map<Object, List<Map<String, Object>>> rightHashMap,
                                                          EqualityJoinCondition joinCondition,
                                                          String leftTableName,
                                                          String rightTableName) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> leftRow : leftRows) {
            Object leftKey = leftRow.get(joinCondition.getLeftColumn());
            List<Map<String, Object>> matchingRightRows = rightHashMap.get(leftKey);

            if (matchingRightRows != null && !matchingRightRows.isEmpty()) {
                for (Map<String, Object> rightRow : matchingRightRows) {
                    Map<String, Object> joinedRow = createJoinedRow(leftRow, rightRow,
                            leftTableName, rightTableName);
                    result.add(joinedRow);
                }
            } else {
                // No match found, add row with null values for right table
                Map<String, Object> joinedRow = createJoinedRowWithNulls(leftRow, null,
                        leftTableName, rightTableName);
                result.add(joinedRow);
            }
        }

        return result;
    }
}