package com.sumit.rdbms.Models;


import java.util.Map;

public class RangeCondition implements Condition {
    private static final long serialVersionUID = 1L;

    private String column;
    private Object minValue;
    private Object maxValue;
    private boolean includeMin;
    private boolean includeMax;

    public RangeCondition() {}

    public RangeCondition(String column, Object minValue, Object maxValue,
                          boolean includeMin, boolean includeMax) {
        this.column = column;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.includeMin = includeMin;
        this.includeMax = includeMax;
    }

    @Override
//    @SuppressWarnings("unchecked")unchecked
    public boolean evaluate(Map<String, Object> row) {
        Object rowValue = row.get(column);
        if (rowValue == null) {
            return false;
        }

        if (!(rowValue instanceof Comparable) ||
                !(minValue instanceof Comparable) ||
                !(maxValue instanceof Comparable)) {
            return false;
        }

        Comparable<Object> value = (Comparable<Object>) rowValue;
        Comparable<Object> min = (Comparable<Object>) minValue;
        Comparable<Object> max = (Comparable<Object>) maxValue;

        int minCompare = value.compareTo(min);
        int maxCompare = value.compareTo(max);

        boolean minSatisfied = includeMin ? minCompare >= 0 : minCompare > 0;
        boolean maxSatisfied = includeMax ? maxCompare <= 0 : maxCompare < 0;

        return minSatisfied && maxSatisfied;
    }

    // Getters and setters
    public String getColumn() { return column; }
    public void setColumn(String column) { this.column = column; }
    public Object getMinValue() { return minValue; }
    public void setMinValue(Object minValue) { this.minValue = minValue; }
    public Object getMaxValue() { return maxValue; }
    public void setMaxValue(Object maxValue) { this.maxValue = maxValue; }
    public boolean isIncludeMin() { return includeMin; }
    public void setIncludeMin(boolean includeMin) { this.includeMin = includeMin; }
    public boolean isIncludeMax() { return includeMax; }
    public void setIncludeMax(boolean includeMax) { this.includeMax = includeMax; }

//    @Override
//    public boolean evaluate(Map<String, Object> row) {
//        return false;
//    }
}