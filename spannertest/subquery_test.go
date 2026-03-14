package spannertest

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

func TestSubqueries(t *testing.T) {
	client, adminClient, _, cleanup := makeClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create Staff table
	err := updateDDL(t, adminClient,
		`CREATE TABLE Staff (
			Tenure INT64,
			ID INT64,
			Name STRING(MAX),
			Cool BOOL,
			Height FLOAT64,
		) PRIMARY KEY (Name, ID)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Staff", []string{"ID", "Name", "Tenure", "Height"}, []interface{}{1, "Jack", 10, 1.85}),
		spanner.Insert("Staff", []string{"ID", "Name", "Tenure", "Height"}, []interface{}{2, "Daniel", 11, 1.83}),
		spanner.Insert("Staff", []string{"Name", "ID", "Cool", "Tenure", "Height"}, []interface{}{"Sam", 3, false, 9, 1.75}),
		spanner.Insert("Staff", []string{"Name", "ID", "Cool", "Tenure", "Height"}, []interface{}{"Teal'c", 4, true, 8, 1.91}),
	})
	if err != nil {
		t.Fatalf("Inserting data: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "scalar subquery - MAX",
			query: `SELECT (SELECT MAX(ID) FROM Staff) AS max_id`,
			want:  [][]interface{}{{int64(4)}},
		},
		{
			name:  "scalar subquery in WHERE",
			query: `SELECT Name FROM Staff WHERE ID > (SELECT AVG(ID) FROM Staff) ORDER BY Name`,
			want: [][]interface{}{
				{"Sam"},
				{"Teal'c"},
			},
		},
		{
			name:  "scalar subquery - COUNT",
			query: `SELECT (SELECT COUNT(*) FROM Staff WHERE Cool = TRUE) AS cool_count`,
			want:  [][]interface{}{{int64(1)}},
		},
		{
			name:  "EXISTS - true",
			query: `SELECT EXISTS(SELECT * FROM Staff WHERE Name = 'Jack') AS has_jack`,
			want:  [][]interface{}{{true}},
		},
		{
			name:  "EXISTS - false",
			query: `SELECT EXISTS(SELECT * FROM Staff WHERE Name = 'Nobody') AS has_nobody`,
			want:  [][]interface{}{{false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := spanner.NewStatement(tt.query)
			ri := client.Single().Query(ctx, stmt)
			got, err := slurpRows(t, ri)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Errorf("Got %d rows, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if len(got[i]) != len(tt.want[i]) {
					t.Errorf("Row %d: got %d columns, want %d", i, len(got[i]), len(tt.want[i]))
					continue
				}
				for j := range got[i] {
					if got[i][j] != tt.want[i][j] {
						t.Errorf("Row %d, col %d: got %v, want %v", i, j, got[i][j], tt.want[i][j])
					}
				}
			}
		})
	}
}
