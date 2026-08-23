package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const maxRows = 1000

var forbidden = regexp.MustCompile(`\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|MERGE|GRANT|REVOKE)\b`)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 4 && args[0] == "gcs-copy" {
		if err := copyGCSObject(context.Background(), args[1], args[2], args[3]); err != nil {
			fail(1, err.Error())
		}
		return
	}
	if len(args) == 1 && args[0] == "ax-tools" {
		fmt.Println(`{"name":"bigquery_query","description":"Run a read-only BigQuery SQL query","parameters":{"type":"object","properties":{"sql":{"type":"string","description":"SELECT or WITH query"}},"required":["sql"]}}`)
		return
	}
	if len(args) != 2 || args[0] != "ax-run" || args[1] != "bigquery_query" {
		fmt.Fprintln(os.Stderr, "usage: bqx ax-tools | bqx ax-run bigquery_query | bqx gcs-copy BUCKET OBJECT FILE")
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

func copyGCSObject(ctx context.Context, bucket, object, destination string) error {
	if bucket == "" || object == "" || destination == "" {
		return errors.New("bucket, object, and file are required")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	client, err := googleHTTPClient(ctx)
	if err != nil {
		return fmt.Errorf("google credentials: %w", err)
	}
	endpoint := "https://storage.googleapis.com/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.QueryEscape(object) + "?alt=media"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download object: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("download object: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 5<<20))
	if err != nil {
		return fmt.Errorf("read object: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return errors.New("downloaded object is empty")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	temporary := destination + ".tmp"
	if err := os.WriteFile(temporary, data, 0644); err != nil {
		return fmt.Errorf("write object: %w", err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("replace object: %w", err)
	}
	return nil
}

func googleHTTPClient(ctx context.Context) (*http.Client, error) {
	credentials := os.Getenv("BQ_CREDENTIALS_JSON")
	if credentials == "" {
		credentials = os.Getenv("ALFRED_BQ_CREDENTIALS_JSON")
	}
	if credentials == "" {
		return google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_only")
	}
	googleCredentials, err := google.CredentialsFromJSON(ctx, []byte(credentials), "https://www.googleapis.com/auth/devstorage.read_only")
	if err != nil {
		return nil, err
	}
	return oauth2.NewClient(ctx, googleCredentials.TokenSource), nil
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
