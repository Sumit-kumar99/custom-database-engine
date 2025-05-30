package com.sumit.rdbms.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class DatabaseConfig {
    @Bean
    @ConfigurationProperties(prefix = "springdb")
    public DatabaseProperties databaseProperties() {
        return new DatabaseProperties();
    }

    public static class DatabaseProperties {
        private String dataPath = "./data";
        private String defaultDatabase = "default";
        private StorageType storageType = StorageType.FILE;

        // Getters and setters
        public String getDataPath() {
            return dataPath;
        }

        public void setDataPath(String dataPath) {
            this.dataPath = dataPath;
        }

        public String getDefaultDatabase() {
            return defaultDatabase;
        }

        public void setDefaultDatabase(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
        }

        public StorageType getStorageType() {
            return storageType;
        }

        public void setStorageType(StorageType storageType) {
            this.storageType = storageType;
        }
    }

    public enum StorageType {
        FILE, MEMORY, KEY_VALUE
    }

}
