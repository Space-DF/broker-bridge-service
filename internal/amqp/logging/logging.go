package logging

import (
	"fmt"
	"log"
)

func Tenant(orgSlug, vhost, emoji, format string, args ...interface{}) {
	log.Printf("[TENANT:%s][VHOST:%s] %s %s", orgSlug, vhost, emoji, fmt.Sprintf(format, args...))
}
