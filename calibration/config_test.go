package calibration

import "testing"

func TestNewConfig(t *testing.T) {
	config := NewConfig()

	if config == nil {
		t.Fatal("NewConfig() returned nil")
	}

	if config.Host != "localhost" {
		t.Errorf("Expected Host to be 'localhost', got %s", config.Host)
	}

	if config.Port != 4000 {
		t.Errorf("Expected Port to be 4000, got %d", config.Port)
	}

	if config.Database != "test" {
		t.Errorf("Expected Database to be 'test', got %s", config.Database)
	}

	if config.User != "root" {
		t.Errorf("Expected User to be 'root', got %s", config.User)
	}

	if config.Password != "" {
		t.Errorf("Expected Password to be empty, got %s", config.Password)
	}
}
