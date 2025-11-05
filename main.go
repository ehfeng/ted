package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/spf13/cobra"
)

var (
	database    string
	host        string
	port        string
	username    string
	password    string
	command     string
	usePostgres bool
	useMySQL    bool
	telemetry   string
	completion  string
)

var rootCmd = &cobra.Command{
	Use:   "ted [database] [table]",
	Short: "ted is a tabular editor for databases",
	Long: `ted is a spreadsheet-like editor for database tables, allowing for easy viewing and editing of table data.

Examples:
  ted test users
  ted mydb.sqlite users
  ted -h localhost -p 5432 -U myuser mydb users`,
	Args: func(cmd *cobra.Command, args []string) error {
		// Allow 0 args if using --telemetry or --completion flags
		if telemetry != "" || completion != "" {
			return nil
		}
		// Otherwise require at least 2 args
		if len(args) < 2 {
			return fmt.Errorf("requires at least 2 arg(s), only received %d", len(args))
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Handle telemetry flag
		if telemetry != "" {
			settings, err := LoadSettings()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading settings: %v\n", err)
				os.Exit(1)
			}

			switch telemetry {
			case "enable":
				settings.TelemetryEnabled = true
				if err := SaveSettings(settings); err != nil {
					fmt.Fprintf(os.Stderr, "Error saving settings: %v\n", err)
					os.Exit(1)
				}
				fmt.Println("Telemetry enabled.")
			case "disable":
				settings.TelemetryEnabled = false
				if err := SaveSettings(settings); err != nil {
					fmt.Fprintf(os.Stderr, "Error saving settings: %v\n", err)
					os.Exit(1)
				}
				fmt.Println("Telemetry disabled.")
			case "status":
				status := "disabled"
				if settings.TelemetryEnabled {
					status = "enabled"
				}
				fmt.Printf("Telemetry status: %s\n", status)
			default:
				fmt.Fprintf(os.Stderr, "Error: invalid telemetry action '%s'. Use 'enable', 'disable', or 'status'\n", telemetry)
				os.Exit(1)
			}
			return
		}

		// Handle completion flag
		if completion != "" {
			switch completion {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				cmd.Root().GenPowerShellCompletion(os.Stdout)
			default:
				fmt.Fprintf(os.Stderr, "Error: invalid shell '%s'. Use 'bash', 'zsh', 'fish', or 'powershell'\n", completion)
				os.Exit(1)
			}
			return
		}

		dbname := args[0]
		tablename := args[1]

		var dbTypeOverride *DatabaseType
		// Validate mutually exclusive flags
		if usePostgres && useMySQL {
			fmt.Fprintln(os.Stderr, "Error: --postgres/--pg and --mysql/--my are mutually exclusive")
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
			DBTypeOverride: dbTypeOverride,
		}

		if err := runEditor(config, dbname, tablename); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
	ValidArgsFunction: completionFunc,
}


func init() {
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database name or file")
	rootCmd.Flags().StringVarP(&host, "host", "h", "", "Database host")
	rootCmd.Flags().StringVarP(&port, "port", "p", "", "Database port")
	rootCmd.Flags().StringVarP(&username, "username", "U", "", "Database username")
	rootCmd.Flags().StringVarP(&password, "password", "W", "", "Database password")
	rootCmd.Flags().StringVarP(&command, "command", "c", "", "SQL command to execute")
	// Database type shorthands
	rootCmd.Flags().BoolVar(&usePostgres, "postgres", false, "Use PostgreSQL for server connections")
	rootCmd.Flags().BoolVar(&usePostgres, "pg", false, "Use PostgreSQL for server connections")
	rootCmd.Flags().BoolVar(&useMySQL, "mysql", false, "Use MySQL for server connections")
	rootCmd.Flags().BoolVar(&useMySQL, "my", false, "Use MySQL for server connections")

	// Telemetry and completion flags
	rootCmd.Flags().StringVar(&telemetry, "telemetry", "", "Manage telemetry settings (enable, disable, status)")
	rootCmd.Flags().StringVar(&completion, "completion", "", "Generate shell completions (bash, zsh, fish, powershell)")

	if err := rootCmd.RegisterFlagCompletionFunc("pg", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"hi", "pg"}, cobra.ShellCompDirectiveNoFileComp
	}); err != nil {
		panic(err)
	}
	if err := rootCmd.RegisterFlagCompletionFunc("mysql", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"hi", "my"}, cobra.ShellCompDirectiveNoFileComp
	}); err != nil {
		panic(err)
	}
	if err := rootCmd.RegisterFlagCompletionFunc("database", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"mysql", "postgres"}, cobra.ShellCompDirectiveNoFileComp
	}); err != nil {
		panic(err)
	}
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

func completionFunc(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	postgres, err := cmd.Flags().GetBool("postgres")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	mysql, err := cmd.Flags().GetBool("mysql")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	// mutually exclusive flags
	if mysql && postgres {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	database, err := cmd.Flags().GetString("database")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	// mutually exclusive flags
	if mysql && postgres && database != "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	postgres = database == "postgres"
	mysql = database == "mysql"

	username, err := cmd.Flags().GetString("username")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	// if username is not set, use current user
	if username == "" {
		currentUser, err := user.Current()
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		username = currentUser.Username
	}
	if len(args) == 0 {
		if postgres {
			// get postgres databases in localhost
			connStr := fmt.Sprintf("host=localhost user=%s dbname=postgres sslmode=disable", username)
			if password != "" {
				connStr = fmt.Sprintf("host=localhost user=%s password=%s dbname=postgres sslmode=disable", username, password)
			}
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer db.Close()
			rows, err := db.Query("SELECT datname FROM pg_database")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer rows.Close()
			results := []string{}
			for rows.Next() {
				var datname string
				err = rows.Scan(&datname)
				if err != nil {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}
				results = append(results, datname)
			}
			return results, cobra.ShellCompDirectiveNoFileComp
		} else if mysql {
			// get mysql databases in localhost
			db, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer db.Close()
			rows, err := db.Query("SHOW DATABASES")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer rows.Close()
			results := []string{}
			for rows.Next() {
				var datname string
				err = rows.Scan(&datname)
				if err != nil {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}
				results = append(results, datname)
			}
			return results, cobra.ShellCompDirectiveNoFileComp
		} else {
			// get sqlite files in current directory
			files, err := os.ReadDir(".")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			results := []string{}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				// sqlite has `SQLite format 3\000` in the first 16 bytes
				buf := make([]byte, 16)
				fi, err := os.Open(file.Name())
				if err != nil {
					continue
				}
				defer fi.Close()
				fi.Read(buf)
				if string(buf) == "SQLite format 3\000" {
					results = append(results, file.Name())
				}
			}
			return results, cobra.ShellCompDirectiveNoFileComp
		}
	} else if len(args) == 1 {
		if postgres {
			// get postgres tables in current database
			dbname := args[0]
			connStr := fmt.Sprintf("host=localhost user=%s dbname=%s sslmode=disable", username, dbname)
			if password != "" {
				connStr = fmt.Sprintf("host=localhost user=%s password=%s dbname=%s sslmode=disable", username, password, dbname)
			}
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer db.Close()
			rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer rows.Close()
			results := []string{}
			for rows.Next() {
				var tableName string
				err = rows.Scan(&tableName)
				if err != nil {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}
				results = append(results, tableName)
			}
			return results, cobra.ShellCompDirectiveNoFileComp
		}
		if mysql {
			// get mysql tables in current database
			db, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(localhost:3306)/", password))
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer db.Close()
			rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", args[0])
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			defer rows.Close()
			results := []string{}
			for rows.Next() {
				var tableName string
				err = rows.Scan(&tableName)
				if err != nil {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}
				results = append(results, tableName)
			}
			return results, cobra.ShellCompDirectiveNoFileComp
		}
		// get sqlite tables in current database
		db, err := sql.Open("sqlite3", args[0])
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		defer rows.Close()
		results := []string{}
		for rows.Next() {
			var tableName string
			err = rows.Scan(&tableName)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			results = append(results, tableName)
		}
		return results, cobra.ShellCompDirectiveNoFileComp
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}


// Sentry DSN (hard-coded)
const SentryDSN = "https://685bea62d5921e602f7adcad1aae6201@o30558.ingest.us.sentry.io/4510273814855680"

func runFirstRunPrompt() error {
	settings, err := LoadSettings()
	if err != nil {
		return err
	}

	// Skip if already completed first run
	if settings.FirstRunComplete {
		return nil
	}

	fmt.Println("Welcome to ted! Let's set up telemetry.")
	fmt.Println()

	// Ask about telemetry
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enable telemetry and error reporting? (y/n) [y]: ")
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(response)
	if response == "" || strings.ToLower(response) == "y" {
		settings.TelemetryEnabled = true
	}

	settings.FirstRunComplete = true

	if err := SaveSettings(settings); err != nil {
		return err
	}

	fmt.Println("Setup complete!")
	fmt.Println()

	return nil
}

func main() {
	log.SetOutput(os.Stderr)

	// Initialize breadcrumbs buffer
	InitBreadcrumbs(100)

	// Run first-run prompt if needed (but skip for telemetry/completion flags or help)
	skipFirstRun := false
	for _, arg := range os.Args[1:] {
		if arg == "help" || arg == "--help" || arg == "-h" ||
			strings.HasPrefix(arg, "--telemetry") || strings.HasPrefix(arg, "--completion") {
			skipFirstRun = true
			break
		}
	}
	if !skipFirstRun {
		if err := runFirstRunPrompt(); err != nil {
			log.Printf("Warning: Could not run first-run setup: %v\n", err)
		}
	}

	// Load settings for telemetry
	settings, err := LoadSettings()
	if err != nil {
		log.Printf("Warning: Could not load settings: %v\n", err)
	} else if settings.TelemetryEnabled {
		if err := InitSentry(SentryDSN); err != nil {
			log.Printf("Warning: Could not initialize Sentry: %v\n", err)
		}
		defer FlushAndShutdown()
	}

	// Set up signal handling for graceful cleanup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		if breadcrumbs != nil {
			breadcrumbs.Flush()
		}
		FlushAndShutdown()
		runCleanup()
		os.Exit(0)
	}()

	// Also run cleanup on normal exit
	defer runCleanup()

	defer func() {
		if err := recover(); err != nil {
			// Capture the panic and send to Sentry
			// Flush any pending breadcrumbs
			if breadcrumbs != nil {
				breadcrumbs.Flush()
			}
			sentry.CurrentHub().Recover(err)
			sentry.Flush(time.Second * 2)
			fmt.Printf("Recovered from panic: %v\n", err)
		}
	}()

	rootCmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for ted")

	if err := rootCmd.Execute(); err != nil {
		if breadcrumbs != nil {
			breadcrumbs.Flush()
		}
		FlushAndShutdown()
		os.Exit(1)
	}
}
