package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	database string
	host     string
	port     string
	username string
	password string
	command  string
)

var rootCmd = &cobra.Command{
	Use:   "ted [database] [table]",
	Short: "ted is a tabular editor for databases",
	Long: `ted is a spreadsheet-like editor for database tables, allowing for easy viewing and editing of table data.

Examples:
  ted test users
  ted mydb.sqlite users
  ted -h localhost -p 5432 -U myuser mydb users`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbname := args[0]
		tablename := args[1]

		config := &Config{
			Database: getValue(database, dbname),
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Command:  command,
		}

		if err := runEditor(config, tablename); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database name or file")
	rootCmd.Flags().StringVarP(&host, "host", "h", "", "Database host")
	rootCmd.Flags().StringVarP(&port, "port", "p", "", "Database port")
	rootCmd.Flags().StringVarP(&username, "username", "U", "", "Database username")
	rootCmd.Flags().StringVarP(&password, "password", "W", "", "Database password")
	rootCmd.Flags().StringVarP(&command, "command", "c", "", "SQL command to execute")
}

func getValue(flag, arg string) string {
	if flag != "" {
		return flag
	}
	return arg
}

var cleanupFuncs []func()

func addCleanup(f func()) {
	cleanupFuncs = append(cleanupFuncs, f)
}

func runCleanup() {
	for _, f := range cleanupFuncs {
		f()
	}
}

func main() {
	log.SetOutput(os.Stderr)

	// Set up signal handling for graceful cleanup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		runCleanup()
		os.Exit(0)
	}()

	// Also run cleanup on normal exit
	defer runCleanup()

	rootCmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for ted")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
