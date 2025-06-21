package com.sumit.rdbms.dto;

public class SQLRequest {
    private String query;

    public SQLRequest() {}

    public SQLRequest(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
