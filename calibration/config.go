package calibration

// Config holds the configuration for optimizer calibration
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// NewConfig creates a new configuration with default values
func NewConfig() *Config {
	return &Config{
		Host:     "localhost",
		Port:     4000,
		Database: "test",
		User:     "root",
		Password: "",
	}
}
