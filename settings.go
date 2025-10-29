package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Settings represents the application configuration
type Settings struct {
	TelemetryEnabled bool `json:"telemetry_enabled"`
	FirstRunComplete bool `json:"first_run_complete"`
}

// getConfigDir returns the configuration directory following XDG Base Directory spec
func getConfigDir() (string, error) {
	// Check XDG_CONFIG_HOME environment variable first
	if xdgHome := os.Getenv("XDG_CONFIG_HOME"); xdgHome != "" {
		return filepath.Join(xdgHome, "ted"), nil
	}

	// Fall back to ~/.config/ted
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not determine home directory: %w", err)
	}

	return filepath.Join(home, ".config", "ted"), nil
}

// getSettingsPath returns the full path to settings.json
func getSettingsPath() (string, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, "settings.json"), nil
}

// EnsureConfigDir creates the configuration directory if it doesn't exist
func EnsureConfigDir() error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return fmt.Errorf("could not create config directory: %w", err)
	}

	return nil
}

// LoadSettings reads the settings.json file, creating it with defaults if it doesn't exist
func LoadSettings() (*Settings, error) {
	if err := EnsureConfigDir(); err != nil {
		return nil, err
	}

	settingsPath, err := getSettingsPath()
	if err != nil {
		return nil, err
	}

	// Check if settings file exists
	if _, err := os.Stat(settingsPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("could not stat settings file: %w", err)
		}
		// File doesn't exist, return default settings (first run)
		return &Settings{
			TelemetryEnabled: false,
			FirstRunComplete: false,
		}, nil
	}

	// Read and parse the settings file
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		return nil, fmt.Errorf("could not read settings file: %w", err)
	}

	var settings Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("could not parse settings file: %w", err)
	}

	return &settings, nil
}

// SaveSettings writes the settings to settings.json
func SaveSettings(settings *Settings) error {
	if err := EnsureConfigDir(); err != nil {
		return err
	}

	settingsPath, err := getSettingsPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal settings: %w", err)
	}

	if err := os.WriteFile(settingsPath, data, 0o644); err != nil {
		return fmt.Errorf("could not write settings file: %w", err)
	}

	return nil
}
