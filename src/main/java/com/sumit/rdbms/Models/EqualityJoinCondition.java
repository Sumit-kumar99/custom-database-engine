package com.sumit.rdbms.Models;

import java.util.Map;
import java.util.Objects;

public class EqualityJoinCondition implements JoinCondition {
    private static final long serialVersionUID = 1L;

    private String leftColumn;
    private String rightColumn;

    public EqualityJoinCondition() {}

    public EqualityJoinCondition(String leftColumn, String rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    @Override
    public boolean matches(Map<String, Object> leftRow, Map<String, Object> rightRow) {
        Object leftValue = leftRow.get(leftColumn);
        Object rightValue = rightRow.get(rightColumn);

        if (leftValue == null && rightValue == null) {
            return true;
        }

        return Objects.equals(leftValue, rightValue);
    }

    @Override
    public String getLeftColumn() {
        return leftColumn;
    }

    @Override
    public String getRightColumn() {
        return rightColumn;
    }

    public void setLeftColumn(String leftColumn) {
        this.leftColumn = leftColumn;
    }

    public void setRightColumn(String rightColumn) {
        this.rightColumn = rightColumn;
    }
}