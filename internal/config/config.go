package config

import (
	"log"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	MQTT      MQTTConfig      `mapstructure:"mqtt"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
}

type ServerConfig struct {
	Host         string        `mapstructure:"host" env:"SERVER_HOST"`
	Port         int           `mapstructure:"port" env:"SERVER_PORT"`
	LogLevel     string        `mapstructure:"log_level" env:"SERVER_LOG_LEVEL"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" env:"SERVER_READ_TIMEOUT"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" env:"SERVER_WRITE_TIMEOUT"`
}

type MQTTConfig struct {
	BrokerURL       string        `mapstructure:"broker_url" env:"MQTT_BROKER_URL"`
	ClientID        string        `mapstructure:"client_id" env:"MQTT_CLIENT_ID"`
	Username        string        `mapstructure:"username" env:"MQTT_USERNAME"`
	Password        string        `mapstructure:"password" env:"MQTT_PASSWORD"`
	Topics          []string      `mapstructure:"topics" env:"MQTT_TOPICS"`
	QoS             byte          `mapstructure:"qos" env:"MQTT_QOS"`
	CleanSession    bool          `mapstructure:"clean_session" env:"MQTT_CLEAN_SESSION"`
	KeepAlive       int           `mapstructure:"keep_alive" env:"MQTT_KEEP_ALIVE"`
	ConnectTimeout  time.Duration `mapstructure:"connect_timeout" env:"MQTT_CONNECT_TIMEOUT"`
	ReconnectDelay  time.Duration `mapstructure:"reconnect_delay" env:"MQTT_RECONNECT_DELAY"`
}


type RateLimitConfig struct {
	Enabled            bool `mapstructure:"enabled" env:"RATE_LIMIT_ENABLED"`
	RequestsPerMinute  int  `mapstructure:"requests_per_minute" env:"RATE_LIMIT_REQUESTS_PER_MINUTE"`
	BurstSize          int  `mapstructure:"burst_size" env:"RATE_LIMIT_BURST_SIZE"`
}


func New() (Config, error) {
	var config Config

	vp := viper.New()

	// Set defaults first (lowest priority)
	setDefaults(vp)

	// Load config file (medium priority)
	vp.SetConfigFile("configs/config.yaml")
	if err := vp.ReadInConfig(); err != nil {
		log.Printf("Config file not found, using defaults and environment variables")
	}

	// Load .env file (higher priority)
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("No .env file found")
	}

	// Enable OS environment variables (highest priority)
	vp.AutomaticEnv()
	vp.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	vp.SetEnvPrefix("")

	return config, vp.Unmarshal(&config)
}

func setDefaults(vp *viper.Viper) {
	// Server defaults
	vp.SetDefault("server.host", "0.0.0.0")
	vp.SetDefault("server.port", 8080)
	vp.SetDefault("server.log_level", "info")
	vp.SetDefault("server.read_timeout", "30s")
	vp.SetDefault("server.write_timeout", "30s")

	// MQTT defaults
	vp.SetDefault("mqtt.broker_url", "tcp://emqx:1883")
	vp.SetDefault("mqtt.client_id", "broker-bridge-service")
	vp.SetDefault("mqtt.username", "admin")
	vp.SetDefault("mqtt.password", "public")
	vp.SetDefault("mqtt.topics", []string{"transformed/device/location"})
	vp.SetDefault("mqtt.qos", 1)
	vp.SetDefault("mqtt.clean_session", true)
	vp.SetDefault("mqtt.keep_alive", 60)
	vp.SetDefault("mqtt.connect_timeout", "10s")
	vp.SetDefault("mqtt.reconnect_delay", "5s")


	// Rate limit defaults
	vp.SetDefault("rate_limit.enabled", true)
	vp.SetDefault("rate_limit.requests_per_minute", 1000)
	vp.SetDefault("rate_limit.burst_size", 100)

}