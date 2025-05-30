package com.sumit.rdbms.service;

import com.sumit.rdbms.Models.Table;
import com.sumit.rdbms.configuration.DatabaseConfig;
import com.sumit.rdbms.exception.DatabaseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class StorageService {



    @Autowired
    private DatabaseConfig.DatabaseProperties properties;


    private final Map<String, Table> memoryTables = new HashMap<>();

    public void saveTable(Table table) {
//        if(table.getName().equals("students")){
//            System.out.println(properties.getStorageType());
//        }
        switch (properties.getStorageType()) {
            case FILE:
                saveTableToFile(table);
                break;
            case MEMORY:
                memoryTables.put(table.getName(), table);
                break;
            case KEY_VALUE:
                saveTableToKeyValue(table);
                break;
            default:
                throw new DatabaseException("Unsupported storage type");
        }
    }

    public Table loadTable(String tableName) {
        switch (properties.getStorageType()) {
            case FILE:
                return loadTableFromFile(tableName);
            case MEMORY:
                return memoryTables.get(tableName);
            case KEY_VALUE:
                return loadTableFromKeyValue(tableName);
            default:
                throw new DatabaseException("Unsupported storage type");
        }
    }

    public void deleteTable(String tableName) {
        switch (properties.getStorageType()) {
            case FILE:
                deleteTableFile(tableName);
                break;
            case MEMORY:
                memoryTables.remove(tableName);
                break;
            case KEY_VALUE:
                deleteTableFromKeyValue(tableName);
                break;
            default:
                throw new DatabaseException("Unsupported storage type");
        }
    }

    public boolean tableExists(String tableName) {
        switch (properties.getStorageType()) {
            case FILE:
                return Files.exists(getTablePath(tableName));
            case MEMORY:
                return memoryTables.containsKey(tableName);
            case KEY_VALUE:
                return tableExistsInKeyValue(tableName);
            default:
                throw new DatabaseException("Unsupported storage type");
        }
    }

    public List<String> listTables() {
        switch (properties.getStorageType()) {
            case FILE:
                return listTableFiles();
            case MEMORY:
                return new ArrayList<>(memoryTables.keySet());
            case KEY_VALUE:
                return listTablesFromKeyValue();
            default:
                throw new DatabaseException("Unsupported storage type");
        }
    }

    // File storage implementations
    private void saveTableToFile(Table table) {
        Path tablePath = getTablePath(table.getName());

        try {
            Files.createDirectories(tablePath.getParent());

            try (ObjectOutputStream oos = new ObjectOutputStream(
                    Files.newOutputStream(tablePath))) {
                oos.writeObject(table);
            }
        } catch (IOException e) {
            throw new DatabaseException("Error saving table: " + e.getMessage(), e);
        }
    }

    private Table loadTableFromFile(String tableName) {
        Path tablePath = getTablePath(tableName);

        try {
            if (!Files.exists(tablePath)) {
                return null;
            }

            try (ObjectInputStream ois = new ObjectInputStream(
                    Files.newInputStream(tablePath))) {
                return (Table) ois.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new DatabaseException("Error loading table: " + e.getMessage(), e);
        }
    }

    private void deleteTableFile(String tableName) {
        Path tablePath = getTablePath(tableName);

        try {
            Files.deleteIfExists(tablePath);
        } catch (IOException e) {
            throw new DatabaseException("Error deleting table: " + e.getMessage(), e);
        }
    }

    private List<String> listTableFiles() {
        Path dbPath = Paths.get(properties.getDataPath(), properties.getDefaultDatabase());

        try {
            if (!Files.exists(dbPath)) {
                return Collections.emptyList();
            }

            return Files.list(dbPath)
                    .filter(path -> path.toString().endsWith(".tbl"))
                    .map(path -> path.getFileName().toString().replace(".tbl", ""))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new DatabaseException("Error listing tables: " + e.getMessage(), e);
        }
    }

    private Path getTablePath(String tableName) {
        return Paths.get(
                properties.getDataPath(),
                properties.getDefaultDatabase(),
                tableName + ".tbl"
        );
    }

    // Key-value storage implementations
    // This is just a placeholder. In a real implementation, you would use a key-value store like RocksDB
    private void saveTableToKeyValue(Table table) {
        // Implementation would connect to a key-value store
        // For now, we'll just use the file storage
        saveTableToFile(table);
    }

    private Table loadTableFromKeyValue(String tableName) {
        // Implementation would connect to a key-value store
        // For now, we'll just use the file storage
        return loadTableFromFile(tableName);
    }

    private void deleteTableFromKeyValue(String tableName) {
        // Implementation would connect to a key-value store
        // For now, we'll just use the file storage
        deleteTableFile(tableName);
    }

    private boolean tableExistsInKeyValue(String tableName) {
        // Implementation would connect to a key-value store
        // For now, we'll just use the file storage
        return Files.exists(getTablePath(tableName));
    }

    private List<String> listTablesFromKeyValue() {
        // Implementation would connect to a key-value store
        // For now, we'll just use the file storage
        return listTableFiles();
    }

}
