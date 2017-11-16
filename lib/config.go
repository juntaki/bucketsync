package bucketsync

type Config struct {
	Bucket        string `yaml:"bucket"`
	Region        string `yaml:"region"`
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	Password      string `yaml:"password"`
	Logging       string `yaml:"logging"`
	LogOutputPath string `yaml:"log_output_path"`
	CacheSize     int    `yaml:"cache_size"`
	ExtentSize    int64  `yaml:"extent_size"`
	Encryption    bool   `yaml:"encryption"`
	Compression   bool   `yaml:"compression"`
}

func (c *Config) validate() bool {
	// TODO
	return true
}
