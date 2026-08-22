package main

import (
	"context"
	"testing"
)

func TestCopyGCSObjectRejectsEmptyArguments(t *testing.T) {
	if err := copyGCSObject(context.Background(), "", "object", "file"); err == nil {
		t.Fatal("empty bucket was accepted")
	}
}

func TestValidateReadOnly(t *testing.T) {
	for _, sql := range []string{"SELECT 1", "  with rows as (select 1) select * from rows"} {
		if err := validateReadOnly(sql); err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
	}
}

func TestValidateReadOnlyRejectsWrites(t *testing.T) {
	for _, sql := range []string{"DELETE FROM users", "SELECT 1; DROP TABLE users", "WITH x AS (UPDATE users SET a = 1) SELECT 1", "MERGE target USING source ON true WHEN MATCHED THEN UPDATE SET a = 1"} {
		if err := validateReadOnly(sql); err == nil {
			t.Fatalf("accepted %q", sql)
		}
	}
}
