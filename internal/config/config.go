package config

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	MQTT      MQTTConfig      `mapstructure:"mqtt"`
	AMQP      AMQPConfig      `mapstructure:"amqp"`
	OrgEvents OrgEventsConfig `mapstructure:"org_events"`
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
	Broker         string        `mapstructure:"broker" env:"MQTT_BROKER"`
	Port           int           `mapstructure:"port" env:"MQTT_PORT"`
	ClientID       string        `mapstructure:"client_id" env:"MQTT_CLIENT_ID"`
	Username       string        `mapstructure:"username" env:"MQTT_USERNAME"`
	Password       string        `mapstructure:"password" env:"MQTT_PASSWORD"`
	Topics         []string      `mapstructure:"topics" env:"MQTT_TOPICS"`
	QoS            byte          `mapstructure:"qos" env:"MQTT_QOS"`
	CleanSession   bool          `mapstructure:"clean_session" env:"MQTT_CLEAN_SESSION"`
	KeepAlive      int           `mapstructure:"keep_alive" env:"MQTT_KEEP_ALIVE"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout" env:"MQTT_CONNECT_TIMEOUT"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay" env:"MQTT_RECONNECT_DELAY"`
}

// GetBrokerURL constructs the MQTT broker URL from broker and port
func (m MQTTConfig) GetBrokerURL() string {
	return fmt.Sprintf("tcp://%s:%d", m.Broker, m.Port)
}

type AMQPConfig struct {
	URL           string   `mapstructure:"url" env:"AMQP_BROKER_URL"`
	AllowedVhosts []string `mapstructure:"allowed_vhosts" env:"AMQP_ALLOWED_VHOSTS"`
	Exchange      string   `mapstructure:"exchange" env:"AMQP_EXCHANGE"`
	Queue         string   `mapstructure:"queue" env:"AMQP_QUEUE"`
	RoutingKey    string   `mapstructure:"routing_key" env:"AMQP_ROUTING_KEY"`
	ConsumerTag   string   `mapstructure:"consumer_tag" env:"AMQP_CONSUMER_TAG"`
	AutoAck       bool     `mapstructure:"auto_ack" env:"AMQP_AUTO_ACK"`
	Exclusive     bool     `mapstructure:"exclusive" env:"AMQP_EXCLUSIVE"`
	NoLocal       bool     `mapstructure:"no_local" env:"AMQP_NO_LOCAL"`
	NoWait        bool     `mapstructure:"no_wait" env:"AMQP_NO_WAIT"`
}

type OrgEventsConfig struct {
	Exchange    string `mapstructure:"exchange" env:"ORG_EVENTS_EXCHANGE"`
	Queue       string `mapstructure:"queue" env:"ORG_EVENTS_QUEUE"`
	RoutingKey  string `mapstructure:"routing_key" env:"ORG_EVENTS_ROUTING_KEY"`
	ConsumerTag string `mapstructure:"consumer_tag" env:"ORG_EVENTS_CONSUMER_TAG"`
}

type RateLimitConfig struct {
	Enabled           bool `mapstructure:"enabled" env:"RATE_LIMIT_ENABLED"`
	RequestsPerMinute int  `mapstructure:"requests_per_minute" env:"RATE_LIMIT_REQUESTS_PER_MINUTE"`
	BurstSize         int  `mapstructure:"burst_size" env:"RATE_LIMIT_BURST_SIZE"`
}

func New() (Config, error) {
	var config Config
	vp := viper.New()

	// Set defaults first (lowest priority)
	setDefaults(vp)

	// Load config file (medium priority)
	vp.SetConfigFile("configs/config.yaml")
	if err := vp.ReadInConfig(); err != nil {
		log.Printf("Config file not found, using defaults and environment variables: %v", err)
	}

	// Load .env file (higher priority) - try multiple paths
	envLoaded := false
	envPaths := []string{".env", "../.env", "../../.env"}
	for _, path := range envPaths {
		if err := godotenv.Load(path); err == nil {
			log.Printf("Loaded .env file from: %s", path)
			envLoaded = true
			break
		}
	}
	if !envLoaded {
		log.Printf("No .env file found in any of the paths: %v", envPaths)
	}

	// Enable OS environment variables (highest priority)
	vp.AutomaticEnv()
	vp.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	vp.SetEnvPrefix("")

	// Bind environment variables manually for proper loading
	bindEnvVars(vp)

	if err := vp.Unmarshal(&config); err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if raw := vp.GetString("amqp.allowed_vhosts"); raw != "" {
		config.AMQP.AllowedVhosts = splitAndTrim(raw)
	}

	// Debug logging for AMQP config
	log.Printf("AMQP URL from config: %s", config.AMQP.URL)

	return config, nil
}

func setDefaults(vp *viper.Viper) {
	// Server defaults
	vp.SetDefault("server.host", "0.0.0.0")
	vp.SetDefault("server.port", 8080)
	vp.SetDefault("server.log_level", "info")
	vp.SetDefault("server.read_timeout", "30s")
	vp.SetDefault("server.write_timeout", "30s")

	// MQTT defaults
	vp.SetDefault("mqtt.broker", "localhost")
	vp.SetDefault("mqtt.port", 1883)
	vp.SetDefault("mqtt.client_id", "broker-bridge-service")
	vp.SetDefault("mqtt.username", "admin")
	vp.SetDefault("mqtt.password", "public")
	vp.SetDefault("mqtt.topics", []string{"transformed/device/location"})
	vp.SetDefault("mqtt.qos", 1) // QoS 1 for guaranteed delivery to broker
	vp.SetDefault("mqtt.clean_session", true)
	vp.SetDefault("mqtt.keep_alive", 60)
	vp.SetDefault("mqtt.connect_timeout", "10s")
	vp.SetDefault("mqtt.reconnect_delay", "5s")

	// AMQP defaults
	vp.SetDefault("amqp.url", "amqp://guest:guest@localhost:5672/")
	vp.SetDefault("amqp.allowed_vhosts", "")
	vp.SetDefault("amqp.auto_ack", false)
	vp.SetDefault("amqp.exclusive", false)
	vp.SetDefault("amqp.no_local", false)
	vp.SetDefault("amqp.no_wait", false)

	// Org events defaults
	vp.SetDefault("org_events.exchange", "org.events")
	vp.SetDefault("org_events.queue", "broker-bridge.org.events.queue")
	vp.SetDefault("org_events.routing_key", "org.#")
	vp.SetDefault("org_events.consumer_tag", "broker-bridge-org-events")

	// Rate limit defaults
	vp.SetDefault("rate_limit.enabled", true)
	vp.SetDefault("rate_limit.requests_per_minute", 1000)
	vp.SetDefault("rate_limit.burst_size", 100)
}

func bindEnvVars(vp *viper.Viper) {
	// Server environment variables
	_ = vp.BindEnv("server.host", "SERVER_HOST")
	_ = vp.BindEnv("server.port", "SERVER_PORT")
	_ = vp.BindEnv("server.log_level", "SERVER_LOG_LEVEL")
	_ = vp.BindEnv("server.read_timeout", "SERVER_READ_TIMEOUT")
	_ = vp.BindEnv("server.write_timeout", "SERVER_WRITE_TIMEOUT")

	// MQTT environment variables
	_ = vp.BindEnv("mqtt.broker", "MQTT_BROKER")
	_ = vp.BindEnv("mqtt.port", "MQTT_PORT")
	_ = vp.BindEnv("mqtt.client_id", "MQTT_CLIENT_ID")
	_ = vp.BindEnv("mqtt.username", "MQTT_USERNAME")
	_ = vp.BindEnv("mqtt.password", "MQTT_PASSWORD")
	_ = vp.BindEnv("mqtt.topics", "MQTT_TOPICS")
	_ = vp.BindEnv("mqtt.qos", "MQTT_QOS")
	_ = vp.BindEnv("mqtt.clean_session", "MQTT_CLEAN_SESSION")
	_ = vp.BindEnv("mqtt.keep_alive", "MQTT_KEEP_ALIVE")
	_ = vp.BindEnv("mqtt.connect_timeout", "MQTT_CONNECT_TIMEOUT")
	_ = vp.BindEnv("mqtt.reconnect_delay", "MQTT_RECONNECT_DELAY")

	// AMQP environment variables
	_ = vp.BindEnv("amqp.url", "AMQP_BROKER_URL")
	_ = vp.BindEnv("amqp.allowed_vhosts", "AMQP_ALLOWED_VHOSTS")
	_ = vp.BindEnv("amqp.exchange", "AMQP_EXCHANGE")
	_ = vp.BindEnv("amqp.queue", "AMQP_QUEUE")
	_ = vp.BindEnv("amqp.routing_key", "AMQP_ROUTING_KEY")
	_ = vp.BindEnv("amqp.consumer_tag", "AMQP_CONSUMER_TAG")
	_ = vp.BindEnv("amqp.auto_ack", "AMQP_AUTO_ACK")
	_ = vp.BindEnv("amqp.exclusive", "AMQP_EXCLUSIVE")
	_ = vp.BindEnv("amqp.no_local", "AMQP_NO_LOCAL")
	_ = vp.BindEnv("amqp.no_wait", "AMQP_NO_WAIT")

	// Org events environment variables
	_ = vp.BindEnv("org_events.exchange", "ORG_EVENTS_EXCHANGE")
	_ = vp.BindEnv("org_events.queue", "ORG_EVENTS_QUEUE")
	_ = vp.BindEnv("org_events.routing_key", "ORG_EVENTS_ROUTING_KEY")
	_ = vp.BindEnv("org_events.consumer_tag", "ORG_EVENTS_CONSUMER_TAG")

	// Org events defaults
	vp.SetDefault("org_events.exchange", "org.events")
	vp.SetDefault("org_events.queue", "broker-bridge.org.events.queue")
	vp.SetDefault("org_events.routing_key", "org.#")
	vp.SetDefault("org_events.consumer_tag", "broker-bridge-org-events")

	// Rate limit environment variables
	_ = vp.BindEnv("rate_limit.enabled", "RATE_LIMIT_ENABLED")
	_ = vp.BindEnv("rate_limit.requests_per_minute", "RATE_LIMIT_REQUESTS_PER_MINUTE")
	_ = vp.BindEnv("rate_limit.burst_size", "RATE_LIMIT_BURST_SIZE")
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
