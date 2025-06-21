package com.sumit.rdbms.Models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.util.Map;


//@JsonTypeInfo(
//        use = JsonTypeInfo.Id.NAME,
//        include = JsonTypeInfo.As.PROPERTY,
//        property = "type"
//)
//@JsonSubTypes({
//        @JsonSubTypes.Type(value = EqualityJoinCondition.class, name = "equals"),
////        @JsonSubTypes.Type(value = CustomJoinCondition.class, name = "custom")
//})
@JsonDeserialize(as = EqualityJoinCondition.class)
public interface JoinCondition extends Serializable {
    boolean matches(Map<String, Object> leftRow, Map<String, Object> rightRow);
    String getLeftColumn();
    String getRightColumn();
}