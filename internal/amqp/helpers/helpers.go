package helpers

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
)

func BuildVhostURL(vhost string, brokerURL string) (string, error) {
	baseURL := brokerURL
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse broker url: %w", err)
	}

	if vhost == "" {
		parsed.Path = "/"
		parsed.RawPath = ""
	} else {
		encoded := "/" + url.PathEscape(vhost)
		parsed.Path = encoded
		parsed.RawPath = encoded
	}

	return parsed.String(), nil
}

func ShouldHandleVhost(vhost string, allowedVhosts []string) bool {
	if len(allowedVhosts) == 0 {
		return true
	}

	return slices.Contains(allowedVhosts, vhost)
}

func MakeConsumerTag(orgSlug, vhost string) string {
	safeVhost := strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(vhost)
	return fmt.Sprintf("%s-broker-bridge-%s-%d", orgSlug, safeVhost, time.Now().UnixNano())
}
