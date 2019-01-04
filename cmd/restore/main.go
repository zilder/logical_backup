package main

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"

	"github.com/jackc/pgx"

	"github.com/ikitiki/logical_backup/pkg/logger"
	"github.com/ikitiki/logical_backup/pkg/logicalrestore"
	"github.com/ikitiki/logical_backup/pkg/message"
)

var (
	pgUser, pgPass, pgHost, pgDbname              *string
	targetTable, backupDir, tableName, schemaName *string
	pgPort                                        *uint
	debug                                         *bool
)

func init() {
	//TODO: switch to go-flags or similar
	pgDbname = flag.String("db", "postgres", "Name of the database to connect to")
	pgUser = flag.String("user", "postgres", "Postgres user name")
	pgPass = flag.String("password", "", "Postgres password")
	pgHost = flag.String("host", "localhost", "Postgres server hostname")
	pgPort = flag.Uint("port", 5432, "Postgres server port")

	tableName = flag.String("table", "", "Source table name")
	schemaName = flag.String("schema", "public", "Schema name")
	targetTable = flag.String("target-table", "", "Target table name (optional)")
	backupDir = flag.String("backup-dir", "", "Backups dir")
	debug = flag.Bool("debug", false, "Toggle debug mode")

	flag.Parse()

	if *tableName == "" || *schemaName == "" || *backupDir == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func main() {

	if _, err := logger.InitGlobalLogger(false); err != nil {
		fmt.Fprintf(os.Stderr, "Could not initialize global logger")
		os.Exit(1)
	}

	if *targetTable != "" {
		zap.S().Infof("restoring into %v", targetTable)
	}

	config := pgx.ConnConfig{
		Database: *pgDbname,
		User:     *pgUser,
		Port:     uint16(*pgPort),
		Password: *pgPass,
		Host:     *pgHost,
	}

	tbl := message.NamespacedName{Namespace: *schemaName, Name: *tableName}
	r, err := logicalrestore.New(tbl, *backupDir, config, *debug)
	if err != nil {
		zap.S().Fatalf("could not create backup logger: %v", err)
	}

	if err := r.Restore(); err != nil {
		zap.S().Fatalf("could not restore table: %v", err)
	}
}
