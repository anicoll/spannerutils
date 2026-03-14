package spannertest

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

// TestTableAliases exercises table aliases (both "AS alias" and bare "alias")
// at the parser and execution level.
func TestTableAliases(t *testing.T) {
	client, adminClient, _, cleanup := makeClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := updateDDL(t, adminClient,
		`CREATE TABLE Staff (
			ID     INT64,
			Name   STRING(MAX),
			Cool   BOOL,
			Tenure INT64,
		) PRIMARY KEY (Name, ID)`); err != nil {
		t.Fatal(err)
	}

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Staff", []string{"ID", "Name", "Cool", "Tenure"}, []interface{}{1, "Jack", nil, 10}),
		spanner.Insert("Staff", []string{"ID", "Name", "Cool", "Tenure"}, []interface{}{2, "Daniel", false, 11}),
		spanner.Insert("Staff", []string{"ID", "Name", "Cool", "Tenure"}, []interface{}{3, "Sam", false, 9}),
		spanner.Insert("Staff", []string{"ID", "Name", "Cool", "Tenure"}, []interface{}{4, "Teal'c", true, 8}),
	})
	if err != nil {
		t.Fatalf("inserting data: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		// ── Basic alias usage ────────────────────────────────────────────────
		{
			name:  "AS alias - qualified column in SELECT",
			query: `SELECT s.Name FROM Staff AS s ORDER BY s.Name`,
			want:  [][]interface{}{{"Daniel"}, {"Jack"}, {"Sam"}, {"Teal'c"}},
		},
		{
			name:  "bare alias - qualified column in SELECT",
			query: `SELECT s.Name FROM Staff s ORDER BY s.Name`,
			want:  [][]interface{}{{"Daniel"}, {"Jack"}, {"Sam"}, {"Teal'c"}},
		},
		{
			name:  "AS alias - qualified column in WHERE",
			query: `SELECT s.Name FROM Staff AS s WHERE s.ID > 2 ORDER BY s.Name`,
			want:  [][]interface{}{{"Sam"}, {"Teal'c"}},
		},
		{
			name:  "bare alias - qualified column in WHERE",
			query: `SELECT s.Name FROM Staff s WHERE s.ID > 2 ORDER BY s.Name`,
			want:  [][]interface{}{{"Sam"}, {"Teal'c"}},
		},
		{
			name:  "alias - mix qualified and unqualified columns",
			query: `SELECT s.Name, Tenure FROM Staff s WHERE s.ID < 3 ORDER BY Name`,
			want:  [][]interface{}{{"Daniel", int64(11)}, {"Jack", int64(10)}},
		},
		// ── Alias in ORDER BY ────────────────────────────────────────────────
		{
			name:  "bare alias - qualified column in ORDER BY",
			query: `SELECT s.Name FROM Staff s ORDER BY s.ID DESC`,
			want:  [][]interface{}{{"Teal'c"}, {"Sam"}, {"Daniel"}, {"Jack"}},
		},
		// ── Self-join using two aliases ──────────────────────────────────────
		{
			name: "self-join with two bare aliases",
			query: `SELECT a.Name FROM Staff a
			        JOIN Staff b ON a.ID = b.ID
			        WHERE b.Tenure > 9
			        ORDER BY a.Name`,
			// Tenure > 9: Jack(10), Daniel(11) — Sam has exactly 9, Teal'c has 8
			want: [][]interface{}{{"Daniel"}, {"Jack"}},
		},
		// ── Alias in EXISTS subquery (correlated) ────────────────────────────
		{
			name: "bare alias in correlated EXISTS",
			query: `SELECT s.Name FROM Staff s
			        WHERE EXISTS (SELECT 1 FROM Staff s2 WHERE s2.ID = s.ID AND s2.Cool = TRUE)
			        ORDER BY s.Name`,
			want: [][]interface{}{{"Teal'c"}},
		},
		// ── Alias in scalar subquery ─────────────────────────────────────────
		{
			name:  "bare alias in scalar subquery",
			query: `SELECT (SELECT MAX(s2.Tenure) FROM Staff s2) AS max_tenure`,
			want:  [][]interface{}{{int64(11)}},
		},
		// ── Column alias (AS in SELECT list) – unchanged behaviour ───────────
		{
			name:  "column alias in SELECT list",
			query: `SELECT Name AS member_name FROM Staff WHERE ID = 1`,
			want:  [][]interface{}{{"Jack"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ri := client.Single().Query(ctx, spanner.NewStatement(tt.query))
			got, err := slurpRows(t, ri)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Errorf("got %d rows, want %d\n got:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
				return
			}
			for i := range got {
				if len(got[i]) != len(tt.want[i]) {
					t.Errorf("row %d: got %d cols, want %d", i, len(got[i]), len(tt.want[i]))
					continue
				}
				for j := range got[i] {
					if got[i][j] != tt.want[i][j] {
						t.Errorf("row %d col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], tt.want[i][j], tt.want[i][j])
					}
				}
			}
		})
	}
}
