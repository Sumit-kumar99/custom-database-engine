package com.sumit.rdbms.Models;

import java.io.Serializable;

public class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private DataType dataType;
    private boolean required;

    public Column() {}

    public Column(String name, DataType dataType, boolean required) {
        this.name = name;
        this.dataType = dataType;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
