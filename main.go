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
	database    string
	host        string
	port        string
	username    string
	password    string
	command     string
	where       string
	orderBy     string
	limitN      int
	usePostgres bool
	useMySQL    bool
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

		var dbTypeOverride *DatabaseType
		// Validate mutually exclusive flags
		if usePostgres && useMySQL {
			fmt.Fprintln(os.Stderr, "Error: --pg and --mysql/--my are mutually exclusive")
			os.Exit(1)
		}
		if database != "" && (usePostgres || useMySQL) {
			fmt.Fprintln(os.Stderr, "Error: -d/--database cannot be used with --pg or --mysql/--my")
			os.Exit(1)
		}
		if usePostgres {
			t := PostgreSQL
			dbTypeOverride = &t
		} else if useMySQL {
			t := MySQL
			dbTypeOverride = &t
		}

		config := &Config{
			Database:       getValue(database, dbname),
			Host:           host,
			Port:           port,
			Username:       username,
			Password:       password,
			Command:        command,
			Where:          where,
			OrderBy:        orderBy,
			DBTypeOverride: dbTypeOverride,
		}

		if err := runEditor(config, dbname, tablename); err != nil {
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
	rootCmd.Flags().StringVarP(&where, "where", "w", "", "WHERE clause to filter rows (without the keyword)")
	rootCmd.Flags().StringVarP(&orderBy, "order-by", "o", "", "ORDER BY clause to sort rows (without the keyword)")
	rootCmd.Flags().IntVarP(&limitN, "limit", "l", 0, "LIMIT number of rows to fetch")
	// Database type shorthands
	rootCmd.Flags().BoolVar(&usePostgres, "pg", false, "Use PostgreSQL for server connections")
	rootCmd.Flags().BoolVar(&useMySQL, "mysql", false, "Use MySQL for server connections")
	rootCmd.Flags().BoolVar(&useMySQL, "my", false, "Use MySQL for server connections (shorthand)")
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
