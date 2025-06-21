package com.sumit.rdbms.dto;

import com.sumit.rdbms.Models.JoinCondition;
import com.sumit.rdbms.Models.JoinType;

public class JoinRequest {
    private String leftTableName;
    private String rightTableName;
    private JoinType joinType;
    private JoinCondition joinCondition;

    public JoinRequest() {}

    public JoinRequest(String leftTableName, String rightTableName,
                       JoinType joinType, JoinCondition joinCondition) {
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
    }

    // Getters and setters
    public String getLeftTableName() {
        return leftTableName;
    }

    public void setLeftTableName(String leftTableName) {
        this.leftTableName = leftTableName;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName) {
        this.rightTableName = rightTableName;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public JoinCondition getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(JoinCondition joinCondition) {
        this.joinCondition = joinCondition;
    }
}