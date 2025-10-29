package main

import (
	"fmt"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
)

// InitSentry initializes the Sentry client with the given DSN
func InitSentry(dsn string) error {
	// Determine environment based on git branch or default to production
	environment := getEnvironment()

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      environment,
		TracesSampleRate: 0.1, // 10% sampling for performance monitoring
		AttachStacktrace: true,
	})

	if err != nil {
		return fmt.Errorf("sentry initialization failed: %w", err)
	}

	// Set the user context if possible
	if user, err := os.UserCacheDir(); err == nil {
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetUser(sentry.User{
				ID: user,
			})
		})
	}

	return nil
}

// getEnvironment determines the environment (dev or production)
func getEnvironment() string {
	// Check if running in dev mode by looking for git directory or environment variables
	if _, err := os.Stat(".git"); err == nil {
		return "development"
	}
	if os.Getenv("TED_ENV") == "dev" {
		return "development"
	}
	return "production"
}

// FlushAndShutdown flushes pending Sentry events and closes the client
func FlushAndShutdown() {
	sentry.Flush(5 * time.Second)
}

// CaptureError sends an error to Sentry along with any pending breadcrumbs
func CaptureError(err error) {
	if err == nil {
		return
	}

	// Flush pending breadcrumbs before capturing the error
	if breadcrumbs != nil {
		breadcrumbs.Flush()
	}

	sentry.CaptureException(err)
}

// CaptureMessage sends a message to Sentry with optional breadcrumbs
func CaptureMessage(message string) {
	// Flush pending breadcrumbs before capturing the message
	if breadcrumbs != nil {
		breadcrumbs.Flush()
	}

	sentry.CaptureMessage(message)
}
