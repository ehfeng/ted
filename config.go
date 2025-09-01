package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]DatabaseConfig `yaml:"databases"`
	Format    map[string]int            `yaml:"format"`
}

type DatabaseConfig struct {
	Type     string                    `yaml:"type"`
	Host     string                    `yaml:"host"`
	Port     string                    `yaml:"port"`
	User     string                    `yaml:"user"`
	DBName   string                    `yaml:"dbname"`
	Format   map[string]interface{}    `yaml:"format"`
}

func loadConfig() (*Config, error) {
	configPath := ".ted.yml"
	
	homeDir, err := os.UserHomeDir()
	if err == nil {
		globalConfigPath := filepath.Join(homeDir, ".ted.yml")
		if _, err := os.Stat(globalConfigPath); err == nil {
			configPath = globalConfigPath
		}
	}
	
	if _, err := os.Stat(".ted.yml"); err == nil {
		configPath = ".ted.yml"
	}
	
	data, err := os.ReadFile(configPath)
	if err != nil {
		return &Config{
			Databases: make(map[string]DatabaseConfig),
			Format:    map[string]int{"int": 3, "text": 12},
		}, nil
	}
	
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing config file %s: %w", configPath, err)
	}
	
	if config.Format == nil {
		config.Format = map[string]int{"int": 3, "text": 12}
	}
	
	for name, dbConfig := range config.Databases {
		if dbConfig.DBName == "" {
			dbConfig.DBName = name
		}
		config.Databases[name] = dbConfig
	}
	
	return &config, nil
}

func (c *Config) GetDatabase(name string) (DatabaseConfig, bool) {
	if db, exists := c.Databases[name]; exists {
		return db, true
	}
	
	if _, err := os.Stat(name); err == nil {
		ext := filepath.Ext(name)
		var dbType string
		switch ext {
		case ".db", ".sqlite", ".sqlite3":
			dbType = "sqlite3"
		case ".ddb", ".duckdb":
			dbType = "duckdb"
		default:
			dbType = "sqlite3"
		}
		
		return DatabaseConfig{
			Type:   dbType,
			DBName: name,
		}, true
	}
	
	return DatabaseConfig{}, false
}