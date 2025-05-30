package com.sumit.rdbms.Models;

import java.io.Serializable;
import java.util.*;

public interface Condition extends Serializable {
    boolean evaluate(Map<String, Object> row);
}
