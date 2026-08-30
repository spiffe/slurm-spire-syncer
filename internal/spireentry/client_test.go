package spireentry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/spiffe/slurm-spire-syncer/internal/config"
	"github.com/spiffe/slurm-spire-syncer/internal/spiretest"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testClient(t *testing.T, f *spiretest.FakeEntryServer, hint string) *Client {
	t.Helper()
	cfg := &config.Config{ClassName: "slurm", Hint: hint}
	return New(spiretest.Start(t, f), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// seeded returns a fake server already holding the given entries.
func seeded(entries ...*types.Entry) *spiretest.FakeEntryServer {
	f := &spiretest.FakeEntryServer{}
	f.Seed(entries...)
	return f
}

func entry(id, hint string) *types.Entry {
	return &types.Entry{
		Id:   id,
		Hint: hint,
		SpiffeId: &types.SPIFFEID{
			TrustDomain: "example.org",
			Path:        "/workload/" + id,
		},
		ParentId: &types.SPIFFEID{
			TrustDomain: "example.org",
			Path:        "/node/node1",
		},
		Selectors: []*types.Selector{{Type: "slurm", Value: "job_id:1"}},
	}
}

func TestOwnsRequiresBothMarkers(t *testing.T) {
	c := testClient(t, &spiretest.FakeEntryServer{}, "slurm")

	cases := []struct {
		name string
		e    *types.Entry
		want bool
	}{
		{"both markers", entry("slurm.abc", "slurm"), true},
		{"foreign ID prefix", entry("otherapp.abc", "slurm"), false},
		{"foreign hint", entry("slurm.abc", "other"), false},
		{"empty hint", entry("slurm.abc", ""), false},
		{"prefix without the separating dot", entry("slurmfoo.abc", "slurm"), false},
		{"nil entry", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.Owns(tc.e); got != tc.want {
				t.Fatalf("Owns() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The hint is pushed down to the server as an indexed exact-match filter so the
// syncer does not have to page the whole entry table.
func TestListManagedSendsHintFilter(t *testing.T) {
	f := seeded(entry("slurm.a", "slurm"))
	c := testClient(t, f, "slurm")

	if _, err := c.ListManaged(context.Background()); err != nil {
		t.Fatalf("ListManaged: %v", err)
	}

	list := f.ListRequests()
	if len(list) != 1 {
		t.Fatalf("got %d ListEntries calls, want 1", len(list))
	}
	byHint := list[0].GetFilter().GetByHint()
	if byHint == nil {
		t.Fatal("ListEntries was sent without a ByHint filter")
	}
	if byHint.GetValue() != "slurm" {
		t.Errorf("ByHint = %q, want %q", byHint.GetValue(), "slurm")
	}
	if list[0].GetOutputMask() == nil || !list[0].GetOutputMask().Hint {
		t.Error("output mask does not request Hint, which ownership depends on")
	}
}

// With the hint disabled there is no server-side handle for ownership, so the
// filter must be omitted entirely rather than sent as an empty string, which
// would match only entries that have no hint at all.
func TestListManagedWithoutHintOmitsFilter(t *testing.T) {
	f := seeded(entry("slurm.a", ""), entry("other.b", ""))
	c := testClient(t, f, "")

	got, err := c.ListManaged(context.Background())
	if err != nil {
		t.Fatalf("ListManaged: %v", err)
	}

	list := f.ListRequests()
	if list[0].GetFilter() != nil {
		t.Errorf("filter = %+v, want none when the hint is disabled", list[0].GetFilter())
	}
	// Ownership falls back to the prefix alone.
	if len(got) != 1 || got[0].Id != "slurm.a" {
		t.Fatalf("ListManaged returned %+v, want only slurm.a", got)
	}
}

func TestListManagedPaginates(t *testing.T) {
	const total = 250
	f := &spiretest.FakeEntryServer{PageSize: 100}
	for i := range total {
		f.Seed(entry(fmt.Sprintf("slurm.%03d", i), "slurm"))
	}
	c := testClient(t, f, "slurm")

	got, err := c.ListManaged(context.Background())
	if err != nil {
		t.Fatalf("ListManaged: %v", err)
	}
	if len(got) != total {
		t.Fatalf("got %d entries, want %d — the page token loop stopped early", len(got), total)
	}

	list := f.ListRequests()
	if len(list) != 3 {
		t.Fatalf("got %d ListEntries calls, want 3 pages", len(list))
	}
	if list[0].GetPageToken() != "" {
		t.Errorf("first request carried page token %q, want empty", list[0].GetPageToken())
	}
	if list[1].GetPageToken() == "" {
		t.Error("second request carried no page token")
	}
}

// The server-side hint filter is not sufficient on its own: another tool could
// stamp the same hint. The prefix check runs client-side over whatever comes
// back.
func TestListManagedFiltersForeignPrefixClientSide(t *testing.T) {
	f := seeded(
		entry("slurm.mine", "slurm"),
		entry("otherapp.theirs", "slurm"),
		entry("slurmfoo.nearly", "slurm"),
	)
	c := testClient(t, f, "slurm")

	got, err := c.ListManaged(context.Background())
	if err != nil {
		t.Fatalf("ListManaged: %v", err)
	}
	if len(got) != 1 || got[0].Id != "slurm.mine" {
		var ids []string
		for _, e := range got {
			ids = append(ids, e.Id)
		}
		t.Fatalf("ListManaged returned %v, want only slurm.mine", ids)
	}
}

func TestListManagedPropagatesRPCError(t *testing.T) {
	f := &spiretest.FakeEntryServer{ListErr: status.Error(codes.Unavailable, "server is down")}
	c := testClient(t, f, "slurm")

	if _, err := c.ListManaged(context.Background()); err == nil {
		t.Fatal("expected an error when ListEntries fails")
	}
}

func TestCreateChunksAtTheBatchBoundary(t *testing.T) {
	f := &spiretest.FakeEntryServer{}
	c := testClient(t, f, "slurm")

	var entries []*types.Entry
	for i := range 250 {
		entries = append(entries, entry(fmt.Sprintf("slurm.%03d", i), "slurm"))
	}

	created, err := c.Create(context.Background(), entries)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created != 250 {
		t.Errorf("created = %d, want 250", created)
	}

	createReqs := f.CreateRequests()
	if len(createReqs) != 3 {
		t.Fatalf("got %d BatchCreateEntry calls, want 3", len(createReqs))
	}
	for i, req := range createReqs {
		if len(req) > batchSize {
			t.Errorf("batch %d carried %d entries, over the %d limit", i, len(req), batchSize)
		}
	}
	if n := len(f.Snapshot()); n != 250 {
		t.Errorf("server holds %d entries, want 250", n)
	}
}

// AlreadyExists carrying a different entry means the server matched a
// pre-existing entry on (spiffe_id, parent_id, selectors) and returned that one
// instead of creating ours. It is not ours, so it is skipped rather than
// adopted, and it is not an error.
func TestCreateSkipsWhenAnotherEntryOwnsTheIdentity(t *testing.T) {
	preexisting := entry("otherapp.xyz", "other")
	f := &spiretest.FakeEntryServer{
		CreateResult: func(*types.Entry) (*types.Status, *types.Entry) {
			return spiretest.Status(codes.AlreadyExists, "similar entry already exists"), preexisting
		},
	}
	c := testClient(t, f, "slurm")

	created, err := c.Create(context.Background(), []*types.Entry{entry("slurm.mine", "slurm")})
	if err != nil {
		t.Fatalf("Create returned an error, want the collision treated as a skip: %v", err)
	}
	if created != 0 {
		t.Errorf("created = %d, want 0", created)
	}
}

// AlreadyExists with no entry attached is the other shape: a duplicate entry ID.
// The next cycle draws a fresh UUID, so this is logged and skipped, not failed.
func TestCreateSkipsOnEntryIDCollision(t *testing.T) {
	f := &spiretest.FakeEntryServer{
		CreateResult: func(*types.Entry) (*types.Status, *types.Entry) {
			return spiretest.Status(codes.AlreadyExists, "entry ID already exists"), nil
		},
	}
	c := testClient(t, f, "slurm")

	created, err := c.Create(context.Background(), []*types.Entry{entry("slurm.mine", "slurm")})
	if err != nil {
		t.Fatalf("Create returned an error, want the ID collision treated as a skip: %v", err)
	}
	if created != 0 {
		t.Errorf("created = %d, want 0", created)
	}
}

func TestCreateReportsRealFailuresButKeepsGoing(t *testing.T) {
	f := &spiretest.FakeEntryServer{
		CreateResult: func(e *types.Entry) (*types.Status, *types.Entry) {
			if e.Id == "slurm.bad" {
				return spiretest.Status(codes.InvalidArgument, "selectors are required"), nil
			}
			return nil, nil
		},
	}
	c := testClient(t, f, "slurm")

	created, err := c.Create(context.Background(), []*types.Entry{
		entry("slurm.good1", "slurm"),
		entry("slurm.bad", "slurm"),
		entry("slurm.good2", "slurm"),
	})

	if created != 2 {
		t.Errorf("created = %d, want the two good entries to succeed", created)
	}
	if err == nil {
		t.Fatal("expected an error naming the failed entry")
	}
	if !strings.Contains(err.Error(), "slurm.bad") {
		t.Errorf("error = %q, want it to name slurm.bad", err)
	}
	if !strings.Contains(err.Error(), "selectors are required") {
		t.Errorf("error = %q, want it to carry the server's message", err)
	}
}

func TestUpdateSendsExplicitInputMask(t *testing.T) {
	f := seeded(entry("slurm.a", "slurm"))
	c := testClient(t, f, "slurm")

	updated, err := c.Update(context.Background(), []*types.Entry{entry("slurm.a", "slurm")})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated != 1 {
		t.Errorf("updated = %d, want 1", updated)
	}

	// A nil input mask means "replace every maskable field", which would clear
	// anything the syncer did not send.
	mask := f.UpdateRequests()[0].GetInputMask()
	if mask == nil {
		t.Fatal("BatchUpdateEntry was sent with a nil InputMask")
	}
	for name, set := range map[string]bool{
		"SpiffeId":    mask.SpiffeId,
		"ParentId":    mask.ParentId,
		"Selectors":   mask.Selectors,
		"X509SvidTtl": mask.X509SvidTtl,
		"JwtSvidTtl":  mask.JwtSvidTtl,
		"Hint":        mask.Hint,
	} {
		if !set {
			t.Errorf("InputMask.%s = false, want the managed field included", name)
		}
	}
}

// An entry deleted underneath the syncer is not a failure: the next list picks
// up the new state.
func TestUpdateToleratesMissingEntry(t *testing.T) {
	c := testClient(t, &spiretest.FakeEntryServer{}, "slurm")

	updated, err := c.Update(context.Background(), []*types.Entry{entry("slurm.gone", "slurm")})
	if err != nil {
		t.Fatalf("Update returned an error for a vanished entry: %v", err)
	}
	if updated != 0 {
		t.Errorf("updated = %d, want 0", updated)
	}
}

func TestUpdateReportsRealFailures(t *testing.T) {
	f := &spiretest.FakeEntryServer{
		UpdateResult: func(*types.Entry) *types.Status {
			return spiretest.Status(codes.Internal, "datastore error")
		},
	}
	c := testClient(t, f, "slurm")

	if _, err := c.Update(context.Background(), []*types.Entry{entry("slurm.a", "slurm")}); err == nil {
		t.Fatal("expected an error for a failed update")
	}
}

// The goal is the entry's absence, so an already-absent entry is a success.
func TestDeleteCountsNotFoundAsSuccess(t *testing.T) {
	f := seeded(entry("slurm.a", "slurm"))
	c := testClient(t, f, "slurm")

	deleted, err := c.Delete(context.Background(), []string{"slurm.a", "slurm.never-existed"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted = %d, want both counted as reaching the desired state", deleted)
	}
	if n := len(f.Snapshot()); n != 0 {
		t.Errorf("server holds %d entries, want 0", n)
	}
}

func TestDeleteChunksAtTheBatchBoundary(t *testing.T) {
	f := &spiretest.FakeEntryServer{}
	var ids []string
	for i := range 250 {
		id := fmt.Sprintf("slurm.%03d", i)
		ids = append(ids, id)
		f.Seed(entry(id, "slurm"))
	}
	c := testClient(t, f, "slurm")

	deleted, err := c.Delete(context.Background(), ids)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != 250 {
		t.Errorf("deleted = %d, want 250", deleted)
	}

	deleteReqs := f.DeleteRequests()
	if len(deleteReqs) != 3 {
		t.Fatalf("got %d BatchDeleteEntry calls, want 3", len(deleteReqs))
	}
	for i, req := range deleteReqs {
		if len(req) > batchSize {
			t.Errorf("batch %d carried %d ids, over the %d limit", i, len(req), batchSize)
		}
	}
}

func TestDeleteReportsRealFailures(t *testing.T) {
	f := &spiretest.FakeEntryServer{
		DeleteResult: func(string) *types.Status {
			return spiretest.Status(codes.PermissionDenied, "not authorized")
		},
	}
	c := testClient(t, f, "slurm")

	if _, err := c.Delete(context.Background(), []string{"slurm.a"}); err == nil {
		t.Fatal("expected an error for a failed delete")
	}
}

func TestEmptyBatchesMakeNoCalls(t *testing.T) {
	f := &spiretest.FakeEntryServer{}
	c := testClient(t, f, "slurm")
	ctx := context.Background()

	if n, err := c.Create(ctx, nil); n != 0 || err != nil {
		t.Errorf("Create(nil) = %d, %v; want 0, nil", n, err)
	}
	if n, err := c.Update(ctx, nil); n != 0 || err != nil {
		t.Errorf("Update(nil) = %d, %v; want 0, nil", n, err)
	}
	if n, err := c.Delete(ctx, nil); n != 0 || err != nil {
		t.Errorf("Delete(nil) = %d, %v; want 0, nil", n, err)
	}

	if len(f.CreateRequests())+len(f.UpdateRequests())+len(f.DeleteRequests()) != 0 {
		t.Error("empty batches still produced RPCs")
	}
}
