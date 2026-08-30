// Package spiretest provides an in-memory stand-in for the SPIRE server's Entry
// API, served over a real unix socket.
//
// Serving it over gRPC rather than stubbing the client interface means the tests
// exercise the generated client, the wire encoding and real gRPC status codes.
// Neither Slurm nor SPIRE can be installed on a development machine, so this is
// as close to the real thing as the test suite gets.
package spiretest

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// FakeEntryServer implements the subset of the Entry API the syncer uses.
//
// It embeds UnimplementedEntryServer so unused RPCs return Unimplemented rather
// than panicking, records every request so tests can assert on what was actually
// sent, and exposes per-result hooks for injecting outcomes that are awkward to
// provoke against a real server — in particular the two distinct AlreadyExists
// shapes BatchCreateEntry can return.
//
// Configure the exported fields before calling Start; they are not guarded.
type FakeEntryServer struct {
	entryv1.UnimplementedEntryServer

	// PageSize forces ListEntries to paginate regardless of the requested page
	// size, so the paging loop can be exercised with a small fixture. Zero means
	// return everything in one page.
	PageSize int

	// ListErr, when set, makes every ListEntries call fail.
	ListErr error

	// CreateResult, UpdateResult and DeleteResult override the outcome for a
	// single item. Returning a nil status falls through to the default
	// in-memory behaviour.
	CreateResult func(*types.Entry) (*types.Status, *types.Entry)
	UpdateResult func(*types.Entry) *types.Status
	DeleteResult func(string) *types.Status

	mu         sync.Mutex
	store      []*types.Entry
	listReqs   []*entryv1.ListEntriesRequest
	createReqs [][]*types.Entry
	updateReqs []*entryv1.BatchUpdateEntryRequest
	deleteReqs [][]string
}

// OK is a successful per-item status.
func OK() *types.Status { return &types.Status{Code: int32(codes.OK), Message: "OK"} }

// Status builds a failing per-item status.
func Status(code codes.Code, msg string) *types.Status {
	return &types.Status{Code: int32(code), Message: msg}
}

// Seed adds entries to the store as if they already existed.
func (f *FakeEntryServer) Seed(entries ...*types.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store = append(f.store, entries...)
}

// Snapshot returns the current contents of the store.
func (f *FakeEntryServer) Snapshot() []*types.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*types.Entry(nil), f.store...)
}

// IDs returns the IDs currently in the store.
func (f *FakeEntryServer) IDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.store))
	for _, e := range f.store {
		ids = append(ids, e.Id)
	}
	return ids
}

// ListRequests returns every ListEntries request received.
func (f *FakeEntryServer) ListRequests() []*entryv1.ListEntriesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*entryv1.ListEntriesRequest(nil), f.listReqs...)
}

// CreateRequests returns the entry batches passed to BatchCreateEntry.
func (f *FakeEntryServer) CreateRequests() [][]*types.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]*types.Entry(nil), f.createReqs...)
}

// UpdateRequests returns every BatchUpdateEntry request received.
func (f *FakeEntryServer) UpdateRequests() []*entryv1.BatchUpdateEntryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*entryv1.BatchUpdateEntryRequest(nil), f.updateReqs...)
}

// DeleteRequests returns the ID batches passed to BatchDeleteEntry.
func (f *FakeEntryServer) DeleteRequests() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.deleteReqs...)
}

func (f *FakeEntryServer) ListEntries(_ context.Context, req *entryv1.ListEntriesRequest) (*entryv1.ListEntriesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listReqs = append(f.listReqs, req)
	if f.ListErr != nil {
		return nil, f.ListErr
	}

	matching := make([]*types.Entry, 0, len(f.store))
	for _, e := range f.store {
		if byHint := req.GetFilter().GetByHint(); byHint != nil && e.Hint != byHint.GetValue() {
			continue
		}
		matching = append(matching, e)
	}

	start := 0
	if tok := req.GetPageToken(); tok != "" {
		n, err := strconv.Atoi(tok)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad page token %q", tok)
		}
		start = n
	}

	size := f.PageSize
	if size <= 0 {
		size = len(matching)
	}
	end := min(start+size, len(matching))

	resp := &entryv1.ListEntriesResponse{Entries: matching[start:end]}
	if end < len(matching) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

func (f *FakeEntryServer) BatchCreateEntry(_ context.Context, req *entryv1.BatchCreateEntryRequest) (*entryv1.BatchCreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.createReqs = append(f.createReqs, req.Entries)

	resp := &entryv1.BatchCreateEntryResponse{}
	for _, e := range req.Entries {
		if f.CreateResult != nil {
			if st, existing := f.CreateResult(e); st != nil {
				resp.Results = append(resp.Results, &entryv1.BatchCreateEntryResponse_Result{
					Status: st, Entry: existing,
				})
				continue
			}
		}
		f.store = append(f.store, e)
		resp.Results = append(resp.Results, &entryv1.BatchCreateEntryResponse_Result{
			Status: OK(), Entry: e,
		})
	}
	return resp, nil
}

func (f *FakeEntryServer) BatchUpdateEntry(_ context.Context, req *entryv1.BatchUpdateEntryRequest) (*entryv1.BatchUpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.updateReqs = append(f.updateReqs, req)

	resp := &entryv1.BatchUpdateEntryResponse{}
	for _, e := range req.Entries {
		if f.UpdateResult != nil {
			if st := f.UpdateResult(e); st != nil {
				resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{Status: st})
				continue
			}
		}
		found := false
		for i, existing := range f.store {
			if existing.Id == e.Id {
				f.store[i] = e
				found = true
				break
			}
		}
		if !found {
			resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{
				Status: Status(codes.NotFound, "entry not found"),
			})
			continue
		}
		resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{
			Status: OK(), Entry: e,
		})
	}
	return resp, nil
}

func (f *FakeEntryServer) BatchDeleteEntry(_ context.Context, req *entryv1.BatchDeleteEntryRequest) (*entryv1.BatchDeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleteReqs = append(f.deleteReqs, req.Ids)

	resp := &entryv1.BatchDeleteEntryResponse{}
	for _, id := range req.Ids {
		if f.DeleteResult != nil {
			if st := f.DeleteResult(id); st != nil {
				resp.Results = append(resp.Results, &entryv1.BatchDeleteEntryResponse_Result{Status: st, Id: id})
				continue
			}
		}
		st := Status(codes.NotFound, "entry not found")
		for i, e := range f.store {
			if e.Id == id {
				f.store = append(f.store[:i], f.store[i+1:]...)
				st = OK()
				break
			}
		}
		resp.Results = append(resp.Results, &entryv1.BatchDeleteEntryResponse_Result{Status: st, Id: id})
	}
	return resp, nil
}

// Start serves f over a unix socket and returns a connected Entry API client.
// The listener and connection are torn down when the test finishes.
func Start(tb testing.TB, f *FakeEntryServer) entryv1.EntryClient {
	tb.Helper()

	// macOS caps unix socket paths near 104 bytes and Go's per-test temp
	// directory names are long enough to exceed that, so the socket goes
	// straight under /tmp with a short prefix.
	dir, err := os.MkdirTemp("/tmp", "sss")
	if err != nil {
		tb.Fatalf("creating socket dir: %v", err)
	}
	tb.Cleanup(func() { os.RemoveAll(dir) })

	socket := filepath.Join(dir, "api.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		tb.Fatalf("listening on %s: %v", socket, err)
	}

	srv := grpc.NewServer()
	entryv1.RegisterEntryServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	tb.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("connecting to %s: %v", socket, err)
	}
	tb.Cleanup(func() { conn.Close() })

	return entryv1.NewEntryClient(conn)
}
