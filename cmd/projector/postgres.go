package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errPostgresConfiguration = errors.New("postgres configuration invalid")

func postgresDSNPath(arguments []string) (string, error) {
	if len(arguments) != 1 || !strings.HasPrefix(arguments[0], "--postgres-dsn-file=") {
		return "", errPostgresConfiguration
	}
	path := strings.TrimPrefix(arguments[0], "--postgres-dsn-file=")
	if path == "" || strings.TrimSpace(path) != path || strings.ContainsAny(path, "\r\n\x00") {
		return "", errPostgresConfiguration
	}
	return path, nil
}

func openProjectorPostgres(ctx context.Context, path string) (*pgxpool.Pool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errPostgresConfiguration
	}
	defer file.Close()
	secret, err := io.ReadAll(io.LimitReader(file, 16*1024+1))
	if err != nil || len(secret) > 16*1024 {
		return nil, errPostgresConfiguration
	}
	dsn := strings.TrimSpace(string(secret))
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") ||
		parsed.Host == "" || parsed.Path == "" || parsed.User == nil {
		return nil, errPostgresConfiguration
	}
	password, present := parsed.User.Password()
	if parsed.User.Username() == "" || !present || password == "" {
		return nil, errPostgresConfiguration
	}
	for key := range parsed.Query() {
		switch strings.ToLower(key) {
		case "service", "servicefile", "passfile", "sslcert", "sslkey", "sslrootcert":
			return nil, errPostgresConfiguration
		}
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errPostgresConfiguration
	}
	config.ConnConfig.Config.Fallbacks = nil
	config.AfterConnect = verifyProjectorPostgresIdentity
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errPostgresConfiguration
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errPostgresConfiguration
	}
	return pool, nil
}

func verifyProjectorPostgresIdentity(ctx context.Context, connection *pgx.Conn) error {
	var sessionUser, currentUser string
	var superuser, bypassRLS bool
	err := connection.QueryRow(ctx, `
		SELECT session_user, current_user, rolsuper, rolbypassrls
		FROM pg_roles WHERE rolname=current_user
	`).Scan(&sessionUser, &currentUser, &superuser, &bypassRLS)
	if err != nil || sessionUser == "" || sessionUser != currentUser || superuser || bypassRLS {
		return errPostgresConfiguration
	}
	return nil
}
