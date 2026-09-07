package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func TestValidateReadOnly(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1",
		"select 1",
		"  SELECT 1  ",
		"with rows as (select 1) select * from rows",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT 1; SELECT 2",
	} {
		if err := validateReadOnly(sql); err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
	}
}

func TestValidateReadOnlyRejectsWrites(t *testing.T) {
	for _, sql := range []string{
		"",
		"   ",
		"DELETE FROM users",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET a = 1",
		"CREATE TABLE t (a INT)",
		"DROP TABLE t",
		"TRUNCATE TABLE t",
		"ALTER TABLE t ADD COLUMN a INT",
		"GRANT SELECT ON t TO u",
		"REVOKE SELECT ON t FROM u",
		"EXPLAIN SELECT 1",
		"SELECT 1; DROP TABLE users",
		"WITH x AS (UPDATE users SET a = 1) SELECT 1",
		"MERGE target USING source ON true WHEN MATCHED THEN UPDATE SET a = 1",
	} {
		if err := validateReadOnly(sql); err == nil {
			t.Fatalf("accepted %q", sql)
		}
	}
}

func TestValidateReadOnlyRejectsKeywordInLiteral(t *testing.T) {
	if err := validateReadOnly("SELECT 'DROP'"); err == nil {
		t.Fatal("accepted query containing DROP inside a string literal")
	}
}

func TestValidateQueryAcceptsSelect(t *testing.T) {
	client := bqClient(t, fakeBQServer(t, "SELECT", 0))
	if err := validateQuery(context.Background(), client, "SELECT 1"); err != nil {
		t.Fatalf("SELECT rejected: %v", err)
	}
}

func TestValidateQueryRejectsNonSelect(t *testing.T) {
	for _, stmt := range []string{"INSERT", "UPDATE", "DELETE", "CREATE_TABLE", "DROP_TABLE", "MERGE"} {
		client := bqClient(t, fakeBQServer(t, stmt, 0))
		err := validateQuery(context.Background(), client, "SELECT 1")
		if err == nil {
			t.Fatalf("statement type %q accepted", stmt)
		}
		if !strings.Contains(err.Error(), "only SELECT and WITH") {
			t.Fatalf("statement type %q: %v", stmt, err)
		}
	}
}

func TestValidateQueryRejectsNonQueryJob(t *testing.T) {
	body := `{"kind":"bigquery#job","jobReference":{"projectId":"proj","jobId":"j"},"configuration":{"load":{}},"status":{"state":"DONE","errors":[]},"statistics":{"load":{}}}`
	client := bqClient(t, fakeJobServer(t, body))
	err := validateQuery(context.Background(), client, "SELECT 1")
	if err == nil {
		t.Fatal("non-query job accepted")
	}
	if !strings.Contains(err.Error(), "only SELECT and WITH") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateQueryReportsRunFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"code":400,"message":"invalid query"}}`)
	}))
	t.Cleanup(srv.Close)
	client := bqClient(t, srv)
	err := validateQuery(context.Background(), client, "SELECT 1")
	if err == nil {
		t.Fatal("expected dry-run failure")
	}
	if !strings.Contains(err.Error(), "validate query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateQueryReportsJobError(t *testing.T) {
	body := `{"kind":"bigquery#job","jobReference":{"projectId":"proj","jobId":"j"},"configuration":{"query":{"query":"q"}},"status":{"state":"DONE","errors":[],"errorResult":{"reason":"invalidQuery","message":"syntax error"}},"statistics":{"query":{"statementType":"SELECT"}}}`
	client := bqClient(t, fakeJobServer(t, body))
	err := validateQuery(context.Background(), client, "SELECT 1")
	if err == nil {
		t.Fatal("job error swallowed")
	}
	if !strings.Contains(err.Error(), "validate query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryRejectsNonSelectBeforeExecution(t *testing.T) {
	client := bqClient(t, fakeBQServer(t, "CREATE_TABLE", 5))
	out, err := query(context.Background(), client, "CREATE TABLE t (a INT)")
	if err == nil {
		t.Fatal("non-SELECT query executed")
	}
	if out != "" {
		t.Fatalf("expected no output on rejection, got %q", out)
	}
}

func TestQueryReturnsAllRowsUnderCap(t *testing.T) {
	client := bqClient(t, fakeBQServer(t, "SELECT", 3))
	out, err := query(context.Background(), client, "SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var parsed struct {
		Rows      []map[string]any `json:"rows"`
		Truncated bool             `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output not JSON object: %v (%q)", err, out)
	}
	if parsed.Truncated {
		t.Fatalf("unexpected truncation: %q", out)
	}
	if len(parsed.Rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(parsed.Rows))
	}
}

func TestQueryTruncatesAtMaxRows(t *testing.T) {
	const total = 1005
	client := bqClient(t, fakeBQServer(t, "SELECT", total))
	out, err := query(context.Background(), client, "SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var parsed struct {
		Rows      []map[string]any `json:"rows"`
		Truncated bool             `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output not JSON object: %v (%q)", err, out)
	}
	if !parsed.Truncated {
		t.Fatalf("expected truncation flag: %q", out)
	}
	if len(parsed.Rows) != maxRows {
		t.Fatalf("want %d rows, got %d", maxRows, len(parsed.Rows))
	}
}

func TestCopyGCSObjectRejectsEmptyArguments(t *testing.T) {
	cases := []struct {
		name   string
		bucket string
		object string
		dest   string
	}{
		{"empty bucket", "", "object", "file"},
		{"empty object", "bucket", "", "file"},
		{"empty destination", "bucket", "object", ""},
		{"all empty", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := copyGCSObject(context.Background(), tc.bucket, tc.object, tc.dest); err == nil {
				t.Fatal("empty argument accepted")
			}
		})
	}
}

func TestCopyGCSObjectAtomicallyReplacesObject(t *testing.T) {
	stubGCS(t, http.StatusOK, "hello world")
	dest := filepath.Join(t.TempDir(), "nested", "out.bin")
	if err := copyGCSObject(context.Background(), "bucket", "object", dest); err != nil {
		t.Fatalf("copyGCSObject: %v", err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("content = %q, want %q", data, "hello world")
	}
	if _, err := os.Stat(dest + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file not replaced: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(dest)); err != nil {
		t.Fatalf("destination directory not created: %v", err)
	}
}

func TestCopyGCSObjectRejectsEmptyDownload(t *testing.T) {
	stubGCS(t, http.StatusOK, "")
	dest := filepath.Join(t.TempDir(), "out.bin")
	err := copyGCSObject(context.Background(), "bucket", "object", dest)
	if err == nil {
		t.Fatal("empty download accepted")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("destination written for empty object: %v", statErr)
	}
}

func TestCopyGCSObjectRejectsHTTPError(t *testing.T) {
	stubGCS(t, http.StatusNotFound, "object gone")
	dest := filepath.Join(t.TempDir(), "out.bin")
	err := copyGCSObject(context.Background(), "bucket", "object", dest)
	if err == nil {
		t.Fatal("HTTP error accepted")
	}
	if !strings.Contains(err.Error(), "download object") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("destination written on HTTP error: %v", statErr)
	}
}

func TestGoogleHTTPClientRequiresCredentials(t *testing.T) {
	t.Setenv("BQ_CREDENTIALS_JSON", "")
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent")
	if _, err := googleHTTPClient(context.Background()); err == nil {
		t.Fatal("googleHTTPClient should fail without any credentials")
	}
}

func TestGoogleHTTPClientAcceptsInlineCredentials(t *testing.T) {
	t.Setenv("BQ_CREDENTIALS_JSON", "")
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", `{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rt"}`)
	if _, err := googleHTTPClient(context.Background()); err != nil {
		t.Fatalf("inline GOOGLE_APPLICATION_CREDENTIALS should be accepted: %v", err)
	}
}

func TestCopyGCSObjectReportsDirectoryError(t *testing.T) {
	stubGCS(t, http.StatusOK, "data")
	tmp := t.TempDir()
	blocker := filepath.Join(tmp, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(blocker, "sub", "out.bin")
	err := copyGCSObject(context.Background(), "bucket", "object", dest)
	if err == nil {
		t.Fatal("expected destination directory creation error")
	}
	if !strings.Contains(err.Error(), "create destination directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewClientCredentialPrecedence(t *testing.T) {
	const valid = `{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rt"}`
	t.Setenv("BQ_CREDENTIALS_JSON", valid)
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", "not-json")
	c, err := newClient(context.Background(), "proj")
	if err != nil {
		t.Fatalf("modern credential should take precedence: %v", err)
	}
	_ = c.Close()

	t.Setenv("BQ_CREDENTIALS_JSON", "")
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", valid)
	c, err = newClient(context.Background(), "proj")
	if err != nil {
		t.Fatalf("legacy credential fallback: %v", err)
	}
	_ = c.Close()

	t.Setenv("BQ_CREDENTIALS_JSON", "")
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", valid)
	c, err = newClient(context.Background(), "proj")
	if err != nil {
		t.Fatalf("GOOGLE_APPLICATION_CREDENTIALS inline JSON should be accepted: %v", err)
	}
	_ = c.Close()

	t.Setenv("BQ_CREDENTIALS_JSON", "")
	t.Setenv("ALFRED_BQ_CREDENTIALS_JSON", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent")
	if _, err := newClient(context.Background(), "proj"); err == nil {
		t.Fatal("client construction should fail without any credentials")
	}
}

func TestMainDescribeProtocol(t *testing.T) {
	code, out, stderr := runMainProcess(t, []string{"describe"}, "", nil)
	if code != 0 {
		t.Fatalf("describe exit %d, stderr=%q", code, stderr)
	}
	var spec struct {
		Name       string `json:"name"`
		Parameters struct {
			Properties struct {
				SQL struct {
					Type string `json:"type"`
				} `json:"sql"`
			} `json:"properties"`
			Required []string `json:"required"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(out), &spec); err != nil {
		t.Fatalf("describe output not JSON: %v (%q)", err, out)
	}
	if spec.Name != "bigquery_query" {
		t.Fatalf("tool name = %q", spec.Name)
	}
	if spec.Parameters.Properties.SQL.Type != "string" {
		t.Fatalf("sql type = %q", spec.Parameters.Properties.SQL.Type)
	}
	if len(spec.Parameters.Required) != 1 || spec.Parameters.Required[0] != "sql" {
		t.Fatalf("required = %v", spec.Parameters.Required)
	}
}

func TestMainUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no args", nil},
		{"unknown subcommand", []string{"foo"}},
		{"run missing command", []string{"run"}},
		{"run wrong command", []string{"run", "other"}},
		{"gcs-copy partial args", []string{"gcs-copy", "bucket"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runMainProcess(t, tc.args, "", nil)
			if code != 2 {
				t.Fatalf("exit = %d, want 2 (stderr=%q)", code, stderr)
			}
			if !strings.Contains(stderr, "usage") {
				t.Fatalf("missing usage message: %q", stderr)
			}
		})
	}
}

func TestMainRunArgumentValidation(t *testing.T) {
	code, _, stderr := runMainProcess(t, []string{"run", "bigquery_query"}, "not json", nil)
	if code != 2 || !strings.Contains(stderr, "invalid arguments") {
		t.Fatalf("invalid JSON: exit=%d stderr=%q", code, stderr)
	}

	code, _, stderr = runMainProcess(t, []string{"run", "bigquery_query"}, `{}`, nil)
	if code != 2 || !strings.Contains(stderr, "only SELECT and WITH queries") {
		t.Fatalf("missing sql: exit=%d stderr=%q", code, stderr)
	}

	code, _, stderr = runMainProcess(t, []string{"run", "bigquery_query"}, `{"sql":"SELECT 1; DROP TABLE t"}`, nil)
	if code != 2 || !strings.Contains(stderr, "forbidden keyword") {
		t.Fatalf("forbidden sql: exit=%d stderr=%q", code, stderr)
	}

	code, _, stderr = runMainProcess(t, []string{"run", "bigquery_query"}, `{"sql":"SELECT 1"}`, nil)
	if code != 2 || !strings.Contains(stderr, "BQ_PROJECT_ID is required") {
		t.Fatalf("missing project: exit=%d stderr=%q", code, stderr)
	}
}

func TestMainProjectEnvResolution(t *testing.T) {
	code, _, stderr := runMainProcess(t, []string{"run", "bigquery_query"}, `{"sql":"SELECT 1"}`, map[string]string{
		"ALFRED_BQ_PROJECT_ID":           "legacy",
		"GOOGLE_APPLICATION_CREDENTIALS": "/nonexistent",
	})
	if strings.Contains(stderr, "BQ_PROJECT_ID is required") {
		t.Fatal("legacy project env ignored")
	}
	if code != 1 {
		t.Fatalf("legacy env: exit=%d, want 1 (stderr=%q)", code, stderr)
	}

	code, _, stderr = runMainProcess(t, []string{"run", "bigquery_query"}, `{"sql":"SELECT 1"}`, map[string]string{
		"BQ_PROJECT_ID":                  "modern",
		"GOOGLE_APPLICATION_CREDENTIALS": "/nonexistent",
	})
	if strings.Contains(stderr, "BQ_PROJECT_ID is required") {
		t.Fatal("modern project env ignored")
	}
	if code != 1 {
		t.Fatalf("modern env: exit=%d, want 1 (stderr=%q)", code, stderr)
	}
}

func TestMainGCSCopyValidation(t *testing.T) {
	code, _, stderr := runMainProcess(t, []string{"gcs-copy", "", "", ""}, "", nil)
	if code != 1 {
		t.Fatalf("exit=%d, want 1 (stderr=%q)", code, stderr)
	}
	if !strings.Contains(stderr, "required") {
		t.Fatalf("missing validation error: %q", stderr)
	}
}

func TestMainProcessHelper(t *testing.T) {
	if os.Getenv("BQX_HELPER_PROCESS") != "1" {
		return
	}
	var args []string
	if s := os.Getenv("BQX_TEST_ARGS"); s != "" {
		args = strings.Split(s, "|")
	}
	os.Args = append([]string{"bqx"}, args...)
	main()
	os.Exit(0)
}

func fakeBQServer(t *testing.T, stmt string, rowCount int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/projects/proj/jobs" && r.Method == http.MethodPost:
			_, _ = fmt.Fprintf(w, `{"kind":"bigquery#job","jobReference":{"projectId":"proj","jobId":"j"},"configuration":{"query":{"query":"q"}},"status":{"state":"DONE","errors":[]},"statistics":{"query":{"statementType":%q}}}`, stmt)
		case r.URL.Path == "/projects/proj/queries" && r.Method == http.MethodPost:
			_, _ = fmt.Fprintf(w, `{"kind":"bigquery#queryResponse","jobReference":{"projectId":"proj","jobId":"j"},"jobComplete":true,"totalRows":%q,"rows":[`, strconv.Itoa(rowCount))
			for i := 0; i < rowCount; i++ {
				if i > 0 {
					_, _ = io.WriteString(w, ",")
				}
				_, _ = fmt.Fprintf(w, `{"f":[{"v":%q}]}`, strconv.Itoa(i))
			}
			_, _ = io.WriteString(w, `],"schema":{"fields":[{"name":"col","type":"INTEGER"}]}}`)
		default:
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fakeJobServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func bqClient(t *testing.T, srv *httptest.Server) *bigquery.Client {
	t.Helper()
	client, err := bigquery.NewClient(context.Background(), "proj", option.WithEndpoint(srv.URL+"/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("bigquery.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func stubGCS(t *testing.T, status int, body string) {
	t.Helper()
	t.Setenv("BQ_CREDENTIALS_JSON", `{"type":"authorized_user","client_id":"cid","client_secret":"csec","refresh_token":"rt"}`)
	orig := http.DefaultTransport
	http.DefaultTransport = rtFunc(func(r *http.Request) (*http.Response, error) {
		resp := &http.Response{
			Header:  make(http.Header),
			Request: r,
		}
		if strings.Contains(r.URL.Host, "oauth2") {
			resp.StatusCode = http.StatusOK
			resp.Status = http.StatusText(http.StatusOK)
			resp.Body = io.NopCloser(strings.NewReader(`{"access_token":"fake","token_type":"Bearer","expires_in":3600}`))
			return resp, nil
		}
		resp.StatusCode = status
		resp.Status = http.StatusText(status)
		resp.Body = io.NopCloser(strings.NewReader(body))
		return resp, nil
	})
	t.Cleanup(func() { http.DefaultTransport = orig })
}

func cleanBQEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		switch key {
		case "BQ_PROJECT_ID", "ALFRED_BQ_PROJECT_ID", "BQ_CREDENTIALS_JSON", "ALFRED_BQ_CREDENTIALS_JSON", "GOOGLE_APPLICATION_CREDENTIALS", "BQX_HELPER_PROCESS", "BQX_TEST_ARGS":
			continue
		}
		out = append(out, kv)
	}
	return out
}

func runMainProcess(t *testing.T, args []string, stdin string, env map[string]string) (int, string, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestMainProcessHelper")
	cmd.Env = append(cleanBQEnv(), "BQX_HELPER_PROCESS=1", "BQX_TEST_ARGS="+strings.Join(args, "|"))
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	return code, stdout.String(), stderr.String()
}
