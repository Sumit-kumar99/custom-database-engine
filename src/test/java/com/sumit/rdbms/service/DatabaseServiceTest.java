package com.sumit.rdbms.service;

import com.sumit.rdbms.Models.Column;
import com.sumit.rdbms.Models.DataType;
import com.sumit.rdbms.Models.EqualCondition;
import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.exception.DatabaseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseServiceTest {

    @InjectMocks
    private DatabaseService databaseService;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        // Clean up any existing test tables
        try {
            List<String> tables = databaseService.listTables();
            for (String table : tables) {
                if (table.startsWith("test_")) {
                    databaseService.dropTable(table);
                }
            }
        } catch (Exception e) {
            // Ignore exceptions during setup
        }
    }

    @Test
    public void testCreateTable() {
        // Arrange
        String tableName = "test_students";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));
        columns.add(new Column("name", DataType.STRING, true));
        columns.add(new Column("gpa", DataType.DOUBLE, false));

        // Act & Assert
        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        // Verify table was created
        assertDoesNotThrow(() -> {
            Table table = databaseService.getTable(tableName);
            assertNotNull(table);
            assertEquals(tableName, table.getName());
            assertEquals(3, table.getColumns().size());
        });
    }

    @Test
    public void testCreateTableAlreadyExists() {
        // Arrange
        String tableName = "test_duplicate";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));

        // Act
        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        // Assert
        assertThrows(DatabaseException.class, () -> {
            databaseService.createTable(tableName, columns);
        });
    }

    @Test
    public void testDropTable() {
        // Arrange
        String tableName = "test_to_drop";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        // Act
        assertDoesNotThrow(() -> {
            databaseService.dropTable(tableName);
        });

        // Assert
        assertThrows(DatabaseException.class, () -> {
            databaseService.getTable(tableName);
        });
    }

    @Test
    public void testDropTableNotExists() {
        // Arrange
        String tableName = "test_nonexistent";

        // Act & Assert
        assertThrows(DatabaseException.class, () -> {
            databaseService.dropTable(tableName);
        });
    }

    @Test
    public void testInsert() {
        // Arrange
        String tableName = "test_insert";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));
        columns.add(new Column("name", DataType.STRING, true));
        columns.add(new Column("active", DataType.BOOLEAN, false));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        Map<String, Object> values = new HashMap<>();
        values.put("id", 1);
        values.put("name", "John Doe");
        values.put("active", true);

        // Act
        assertDoesNotThrow(() -> {
            databaseService.insert(tableName, values);
        });

        // Assert
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> results = databaseService.select(tableName, null);
            assertEquals(1, results.size());
            assertEquals(1, results.get(0).get("id"));
            assertEquals("John Doe", results.get(0).get("name"));
            assertEquals(true, results.get(0).get("active"));
        });
    }

    @Test
    public void testInsertMissingRequiredColumn() {
        // Arrange
        String tableName = "test_insert_missing_required";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));
        columns.add(new Column("name", DataType.STRING, true));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        Map<String, Object> values = new HashMap<>();
        values.put("id", 1);
        // Missing required "name" column

        // Act & Assert
        assertThrows(DatabaseException.class, () -> {
            databaseService.insert(tableName, values);
        });
    }

    @Test
    public void testInsertInvalidDataType() {
        // Arrange
        String tableName = "test_insert_invalid_type";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        Map<String, Object> values = new HashMap<>();
        values.put("id", "not_an_integer"); // String instead of Integer

        // Act & Assert
        assertThrows(DatabaseException.class, () -> {
            databaseService.insert(tableName, values);
        });
    }

    @Test
    public void testSelectWithCondition() {
        // Arrange
        String tableName = "test_select_with_condition";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));
        columns.add(new Column("name", DataType.STRING, true));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        // Insert test data
        Map<String, Object> values1 = new HashMap<>();
        values1.put("id", 1);
        values1.put("name", "Alice");

        Map<String, Object> values2 = new HashMap<>();
        values2.put("id", 2);
        values2.put("name", "Bob");

        assertDoesNotThrow(() -> {
            databaseService.insert(tableName, values1);
            databaseService.insert(tableName, values2);
        });

        // Create condition
        EqualCondition condition = new EqualCondition("name", "Alice");

        // Act
        List<Map<String, Object>> results = null;
        try {
            results = databaseService.select(tableName, condition);
        } catch (DatabaseException e) {
            fail("Select with condition threw an exception: " + e.getMessage());
        }

        // Assert
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).get("id"));
        assertEquals("Alice", results.get(0).get("name"));
    }

    @Test
    public void testListTables() {
        // Arrange
        String tableName1 = "test_list_tables1";
        String tableName2 = "test_list_tables2";

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName1, columns);
            databaseService.createTable(tableName2, columns);
        });

        // Act
        List<String> tables = null;
        try {
            tables = databaseService.listTables();
        } catch (DatabaseException e) {
            fail("List tables threw an exception: " + e.getMessage());
        }

        // Assert
        assertNotNull(tables);
        assertTrue(tables.contains(tableName1));
        assertTrue(tables.contains(tableName2));
    }

    @Test
    public void testGetTableSchema() {
        // Arrange
        String tableName = "test_schema";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", DataType.INTEGER, true));
        columns.add(new Column("name", DataType.STRING, false));
        columns.add(new Column("date", DataType.DATE, false));

        assertDoesNotThrow(() -> {
            databaseService.createTable(tableName, columns);
        });

        // Act
        Table table = null;
        try {
            table = databaseService.getTable(tableName);
        } catch (DatabaseException e) {
            fail("Get table schema threw an exception: " + e.getMessage());
        }

        // Assert
        assertNotNull(table);
        assertEquals(tableName, table.getName());
        assertEquals(3, table.getColumns().size());

        // Check column details
        boolean foundIdColumn = false;
        boolean foundNameColumn = false;
        boolean foundDateColumn = false;

        for (Column column : table.getColumns()) {
            if ("id".equals(column.getName())) {
                foundIdColumn = true;
                assertEquals(DataType.INTEGER, column.getDataType());
                assertTrue(column.isRequired());
            } else if ("name".equals(column.getName())) {
                foundNameColumn = true;
                assertEquals(DataType.STRING, column.getDataType());
                assertFalse(column.isRequired());
            } else if ("date".equals(column.getName())) {
                foundDateColumn = true;
                assertEquals(DataType.DATE, column.getDataType());
                assertFalse(column.isRequired());
            }
        }

        assertTrue(foundIdColumn);
        assertTrue(foundNameColumn);
        assertTrue(foundDateColumn);
    }
}
