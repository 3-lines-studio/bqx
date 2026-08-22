package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const maxRows = 1000

var forbidden = regexp.MustCompile(`\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|MERGE|GRANT|REVOKE)\b`)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "ax-tools" {
		fmt.Println(`{"name":"bigquery_query","description":"Run a read-only BigQuery SQL query","parameters":{"type":"object","properties":{"sql":{"type":"string","description":"SELECT or WITH query"}},"required":["sql"]}}`)
		return
	}
	if len(os.Args) != 3 || os.Args[1] != "ax-run" || os.Args[2] != "bigquery_query" {
		fmt.Fprintln(os.Stderr, "usage: bqx ax-tools | bqx ax-run bigquery_query")
		os.Exit(2)
	}
	var input struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20)).Decode(&input); err != nil {
		fail(2, "invalid arguments: "+err.Error())
	}
	if err := validateReadOnly(input.SQL); err != nil {
		fail(2, err.Error())
	}
	project := os.Getenv("BQ_PROJECT_ID")
	if project == "" {
		project = os.Getenv("ALFRED_BQ_PROJECT_ID")
	}
	if project == "" {
		fail(2, "BQ_PROJECT_ID is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := newClient(ctx, project)
	if err != nil {
		fail(1, err.Error())
	}
	defer client.Close()
	result, err := query(ctx, client, input.SQL)
	if err != nil {
		fail(1, err.Error())
	}
	fmt.Println(result)
}

func newClient(ctx context.Context, project string) (*bigquery.Client, error) {
	credentials := os.Getenv("BQ_CREDENTIALS_JSON")
	if credentials == "" {
		credentials = os.Getenv("ALFRED_BQ_CREDENTIALS_JSON")
	}
	if credentials == "" {
		return bigquery.NewClient(ctx, project)
	}
	return bigquery.NewClient(ctx, project, option.WithCredentialsJSON([]byte(credentials)))
}

func query(ctx context.Context, client *bigquery.Client, sql string) (string, error) {
	if err := validateQuery(ctx, client, sql); err != nil {
		return "", err
	}
	rows, err := client.Query(sql).Read(ctx)
	if err != nil {
		return "", fmt.Errorf("query: %w", err)
	}
	result := make([]map[string]bigquery.Value, 0)
	truncated := false
	for {
		if len(result) == maxRows {
			truncated = true
			break
		}
		var row map[string]bigquery.Value
		err := rows.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("read row: %w", err)
		}
		result = append(result, row)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("encode result: %w", err)
	}
	output := string(data)
	if truncated {
		output += "\nResults truncated at 1000 rows."
	}
	return output, nil
}

func validateQuery(ctx context.Context, client *bigquery.Client, sql string) error {
	query := client.Query(sql)
	query.DryRun = true
	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("validate query: %w", err)
	}
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return fmt.Errorf("validate query: %w", err)
	}
	stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok || stats.StatementType != "SELECT" {
		return errors.New("only SELECT queries are allowed")
	}
	return nil
}

func validateReadOnly(sql string) error {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return errors.New("only SELECT and WITH queries are allowed")
	}
	if keyword := forbidden.FindString(upper); keyword != "" {
		return fmt.Errorf("query contains forbidden keyword: %s", keyword)
	}
	return nil
}

func fail(code int, message string) {
	fmt.Fprintln(os.Stderr, "error: "+message)
	os.Exit(code)
}
