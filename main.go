package main

import (
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "ted [dbname] [table.col,col]",
	Short: "ted is a tabular editor for databases",
	Long: `ted is a tabular editor that displays database tables as markdown tables
and provides spreadsheet-like editing capabilities with mouse support.

Examples:
  ted test users.id,name
  ted test -c "select * from users where name = 'eric'"`,
	Args: cobra.MaximumNArgs(2),
	Run:  runTed,
}

var (
	database string
	host     string
	port     string
	username string
	password string
	command  string
)

func init() {
	rootCmd.Flags().BoolP("help", "", false, "help for ted")
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database name")
	rootCmd.Flags().StringVarP(&host, "host", "h", "", "Database host")
	rootCmd.Flags().StringVarP(&port, "port", "p", "", "Database port")
	rootCmd.Flags().StringVarP(&username, "username", "U", "", "Database username")
	rootCmd.Flags().StringVarP(&password, "password", "W", "", "Database password")
	rootCmd.Flags().StringVarP(&command, "command", "c", "", "SQL command to execute")
}

func runTed(cmd *cobra.Command, args []string) {
	config, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}
	
	var dbName string
	var query string
	
	if len(args) >= 1 {
		dbName = args[0]
	}
	
	if command != "" {
		query = command
	} else if len(args) >= 2 {
		parts := strings.Split(args[1], ".")
		if len(parts) == 2 {
			table := parts[0]
			columns := parts[1]
			query = fmt.Sprintf("SELECT %s FROM %s", columns, table)
		} else {
			query = fmt.Sprintf("SELECT * FROM %s", args[1])
		}
	} else {
		fmt.Fprintf(os.Stderr, "Error: must specify either a table or use -c flag with SQL command\n")
		os.Exit(1)
	}
	
	var dbConfig DatabaseConfig
	var found bool
	
	if dbName != "" {
		dbConfig, found = config.GetDatabase(dbName)
		if !found {
			fmt.Fprintf(os.Stderr, "Database '%s' not found in config and not a valid file\n", dbName)
			os.Exit(1)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Error: must specify database name\n")
		os.Exit(1)
	}
	
	connectionFlags := ConnectionFlags{
		Database: database,
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}
	
	connection, err := connectToDatabase(dbConfig, connectionFlags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Database connection failed: %v\n", err)
		os.Exit(1)
	}
	defer connection.DB.Close()
	
	model := NewModel(config, connection)
	
	p := tea.NewProgram(model, tea.WithAltScreen())
	
	go func() {
		p.Send(loadTableData(connection, query)())
	}()
	
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running program: %v\n", err)
		os.Exit(1)
	}
}