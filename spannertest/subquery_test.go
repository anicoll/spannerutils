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

// TestTrailingCommaBeforeFrom verifies that a trailing comma in the SELECT
// list (e.g. after the last aliased column or subquery) does not cause the
// parser to consume FROM as a column identifier, which previously left the
// outer FROM clause as "unexpected trailing content".
func TestTrailingCommaBeforeFrom(t *testing.T) {
	client, adminClient, _, cleanup := makeClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := updateDDL(t, adminClient,
		`CREATE TABLE Items (
			ID    INT64 NOT NULL,
			Name  STRING(MAX) NOT NULL,
			Value INT64 NOT NULL,
		) PRIMARY KEY (ID)`); err != nil {
		t.Fatal(err)
	}

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items", []string{"ID", "Name", "Value"}, []interface{}{1, "alpha", 10}),
		spanner.Insert("Items", []string{"ID", "Name", "Value"}, []interface{}{2, "beta", 20}),
	})
	if err != nil {
		t.Fatalf("inserting data: %v", err)
	}

	// The trailing comma after Value is the regression trigger.
	stmt := spanner.NewStatement(`SELECT i.ID, i.Value, FROM Items AS i ORDER BY i.ID`)
	ri := client.Single().Query(ctx, stmt)
	got, err := slurpRows(t, ri)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	want := [][]interface{}{
		{int64(1), int64(10)},
		{int64(2), int64(20)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d\n got:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		for j := range got[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %v, want %v", i, j, got[i][j], want[i][j])
			}
		}
	}
}

// TestCorrelatedSubqueryWithJoinHint exercises:
//  1. A correlated scalar subquery in the SELECT list that has its own FROM,
//     WHERE (with IN UNNEST), and GROUP BY clauses.
//  2. An INNER JOIN whose RHS table carries a @{FORCE_INDEX=…} hint.
//
// These two features together caused the parser to misidentify the outer FROM
// as unexpected trailing content.
func TestCorrelatedSubqueryWithJoinHint(t *testing.T) {
	client, adminClient, _, cleanup := makeClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Schema: Schedules, Tasks, PaymentResources.
	if err := updateDDL(t, adminClient,
		`CREATE TABLE Schedules (
			ID        INT64 NOT NULL,
			ChannelID INT64 NOT NULL,
		) PRIMARY KEY (ID)`,
		`CREATE TABLE Tasks (
			ScheduleID  INT64 NOT NULL,
			ID          INT64 NOT NULL,
			PlannedTime INT64 NOT NULL,
			Status      STRING(MAX) NOT NULL,
		) PRIMARY KEY (ScheduleID, ID)`,
		`CREATE TABLE PaymentResources (
			ScheduleID INT64 NOT NULL,
			ID         INT64 NOT NULL,
		) PRIMARY KEY (ScheduleID, ID)`,
		`CREATE INDEX PaymentResourcesIdx ON PaymentResources (ScheduleID)`,
	); err != nil {
		t.Fatal(err)
	}

	// Schedules: ch100 has IDs 1 and 2; ch200 has ID 3.
	// Tasks for schedule 1: PlannedTime 10 (PENDING), 20 (DONE).
	// Tasks for schedule 2: PlannedTime 5 (PENDING), 30 (PENDING).
	// Tasks for schedule 3: PlannedTime 15 (PENDING).
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Schedules", []string{"ID", "ChannelID"}, []interface{}{1, 100}),
		spanner.Insert("Schedules", []string{"ID", "ChannelID"}, []interface{}{2, 100}),
		spanner.Insert("Schedules", []string{"ID", "ChannelID"}, []interface{}{3, 200}),

		spanner.Insert("Tasks", []string{"ScheduleID", "ID", "PlannedTime", "Status"}, []interface{}{1, 1, 10, "PENDING"}),
		spanner.Insert("Tasks", []string{"ScheduleID", "ID", "PlannedTime", "Status"}, []interface{}{1, 2, 20, "DONE"}),
		spanner.Insert("Tasks", []string{"ScheduleID", "ID", "PlannedTime", "Status"}, []interface{}{2, 3, 5, "PENDING"}),
		spanner.Insert("Tasks", []string{"ScheduleID", "ID", "PlannedTime", "Status"}, []interface{}{2, 4, 30, "PENDING"}),
		spanner.Insert("Tasks", []string{"ScheduleID", "ID", "PlannedTime", "Status"}, []interface{}{3, 5, 15, "PENDING"}),

		spanner.Insert("PaymentResources", []string{"ScheduleID", "ID"}, []interface{}{1, 1}),
		spanner.Insert("PaymentResources", []string{"ScheduleID", "ID"}, []interface{}{2, 1}),
		spanner.Insert("PaymentResources", []string{"ScheduleID", "ID"}, []interface{}{3, 1}),
	})
	if err != nil {
		t.Fatalf("inserting data: %v", err)
	}

	// Query uses:
	//   - correlated scalar subquery in SELECT list (with FROM, WHERE+IN UNNEST, GROUP BY)
	//   - INNER JOIN with @{FORCE_INDEX=…} table hint on the RHS
	//   - outer WHERE with IN UNNEST(@channel_ids)
	// Only channel 100 schedules (IDs 1, 2) should be returned.
	// Schedule 1: min PENDING PlannedTime = 10.
	// Schedule 2: min PENDING PlannedTime = 5.
	stmt := spanner.Statement{
		SQL: `SELECT s.ID,
			(SELECT MIN(t.PlannedTime)
			 FROM Tasks t
			 WHERE t.ScheduleID = s.ID
			   AND t.Status IN UNNEST(@statuses)
			 GROUP BY t.ScheduleID) AS NextPlannedTime
		FROM Schedules AS s
		INNER JOIN PaymentResources@{FORCE_INDEX=PaymentResourcesIdx} pr ON pr.ScheduleID = s.ID
		WHERE s.ChannelID IN UNNEST(@channel_ids)
		ORDER BY s.ID`,
		Params: map[string]interface{}{
			"statuses":    []string{"PENDING"},
			"channel_ids": []int64{100},
		},
	}

	ri := client.Single().Query(ctx, stmt)
	got, err := slurpRows(t, ri)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	want := [][]interface{}{
		{int64(1), int64(10)},
		{int64(2), int64(5)},
	}

	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d\n got:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			t.Errorf("row %d: got %d cols, want %d", i, len(got[i]), len(want[i]))
			continue
		}
		for j := range got[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %v (%T), want %v (%T)",
					i, j, got[i][j], got[i][j], want[i][j], want[i][j])
			}
		}
	}
}

// TestGreatestLeast covers the GREATEST and LEAST scalar functions.
func TestGreatestLeast(t *testing.T) {
	client, adminClient, _, cleanup := makeClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := updateDDL(t, adminClient,
		`CREATE TABLE Vals (
			ID    INT64 NOT NULL,
			A     INT64 NOT NULL,
			B     INT64 NOT NULL,
			C     INT64 NOT NULL,
		) PRIMARY KEY (ID)`); err != nil {
		t.Fatal(err)
	}

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Vals", []string{"ID", "A", "B", "C"}, []interface{}{1, 3, 1, 2}),
		spanner.Insert("Vals", []string{"ID", "A", "B", "C"}, []interface{}{2, 5, 5, 5}),
		spanner.Insert("Vals", []string{"ID", "A", "B", "C"}, []interface{}{3, 7, 9, 4}),
	})
	if err != nil {
		t.Fatalf("inserting data: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "GREATEST of three columns",
			query: `SELECT GREATEST(A, B, C) FROM Vals ORDER BY ID`,
			want:  [][]interface{}{{int64(3)}, {int64(5)}, {int64(9)}},
		},
		{
			name:  "LEAST of three columns",
			query: `SELECT LEAST(A, B, C) FROM Vals ORDER BY ID`,
			want:  [][]interface{}{{int64(1)}, {int64(5)}, {int64(4)}},
		},
		{
			name:  "GREATEST with literals",
			query: `SELECT GREATEST(1, 2, 3)`,
			want:  [][]interface{}{{int64(3)}},
		},
		{
			name:  "LEAST with literals",
			query: `SELECT LEAST(10, 2, 7)`,
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name:  "GREATEST returns NULL on any NULL arg",
			query: `SELECT GREATEST(1, NULL, 3)`,
			want:  [][]interface{}{{nil}},
		},
		{
			name:  "LEAST returns NULL on any NULL arg",
			query: `SELECT LEAST(NULL, 2, 3)`,
			want:  [][]interface{}{{nil}},
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
				t.Fatalf("got %d rows, want %d\n got:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if len(got[i]) != len(tt.want[i]) {
					t.Errorf("row %d: got %d cols, want %d", i, len(got[i]), len(tt.want[i]))
					continue
				}
				for j := range got[i] {
					if got[i][j] != tt.want[i][j] {
						t.Errorf("row %d col %d: got %v (%T), want %v (%T)",
							i, j, got[i][j], got[i][j], tt.want[i][j], tt.want[i][j])
					}
				}
			}
		})
	}
}
