// Package spireentry wraps the SPIRE server Entry API with the listing,
// ownership and batching rules this syncer needs.
package spireentry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spiffe/slurm-spire-syncer/internal/config"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	// listPageSize is the page size requested from ListEntries. The API returns
	// a next_page_token even when no page size was requested, so the paging loop
	// runs regardless; setting it explicitly just bounds each response.
	listPageSize = 500

	// batchSize caps the entries sent in a single Batch*Entry call.
	batchSize = 100
)

// managedFieldMask is the set of entry fields this syncer owns. It is used both
// as the ListEntries output mask and as the BatchUpdateEntry input mask.
//
// Passing a nil input mask to BatchUpdateEntry means "replace every maskable
// field", which would clear anything not sent. The mask is therefore always
// explicit.
func managedFieldMask() *types.EntryMask {
	return &types.EntryMask{
		SpiffeId:    true,
		ParentId:    true,
		Selectors:   true,
		X509SvidTtl: true,
		JwtSvidTtl:  true,
		Hint:        true,
	}
}

// Dial opens a connection to the SPIRE server's private API socket.
//
// The socket is plaintext and unauthenticated at the transport layer: the SPIRE
// server tags every unix-domain peer as a local caller and its default
// authorization policy grants local callers full access to the Entry API. Access
// is gated by filesystem permissions on the socket (mode 0770), so this process
// must run as the server's user or share its group.
func Dial(socket string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("spireentry: connecting to %s: %w", socket, err)
	}
	return conn, nil
}

// Client is a thin, ownership-aware wrapper around entryv1.EntryClient.
type Client struct {
	api    entryv1.EntryClient
	hint   string
	prefix string
	log    *slog.Logger
}

// New builds a Client over an existing Entry API client.
func New(api entryv1.EntryClient, cfg *config.Config, log *slog.Logger) *Client {
	return &Client{
		api:    api,
		hint:   cfg.Hint,
		prefix: cfg.EntryIDPrefix(),
		log:    log,
	}
}

// Owns reports whether an entry belongs to this syncer.
//
// Ownership requires both markers: the configured hint and the "<className>."
// entry ID prefix. The hint alone is not sufficient, because nothing stops
// another tool from stamping the same hint; the prefix alone would mean a full
// table scan. Requiring both is what makes deletion safe — an entry matching
// only one marker is never updated and never deleted.
func (c *Client) Owns(e *types.Entry) bool {
	if e == nil || !strings.HasPrefix(e.Id, c.prefix) {
		return false
	}
	return e.Hint == c.hint
}

// ListManaged returns every entry this syncer owns.
//
// When a hint is configured it is pushed down to the server as an indexed
// exact-match filter. With hint disabled ("") there is no server-side handle for
// ownership, so the full entry list is paged and filtered on the prefix alone.
func (c *Client) ListManaged(ctx context.Context) ([]*types.Entry, error) {
	req := &entryv1.ListEntriesRequest{
		OutputMask: managedFieldMask(),
		PageSize:   listPageSize,
	}
	if c.hint != "" {
		req.Filter = &entryv1.ListEntriesRequest_Filter{
			ByHint: wrapperspb.String(c.hint),
		}
	}

	var out []*types.Entry
	for {
		resp, err := c.api.ListEntries(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("spireentry: listing entries: %w", err)
		}
		for _, e := range resp.Entries {
			if c.Owns(e) {
				out = append(out, e)
			}
		}
		if resp.NextPageToken == "" {
			return out, nil
		}
		req.PageToken = resp.NextPageToken
	}
}

// Create creates entries, returning the number actually created.
//
// BatchCreateEntry reports two distinct AlreadyExists conditions and they need
// different handling:
//
//   - Result.Entry set, with an ID other than the one requested: the server
//     found a pre-existing entry with the same (spiffe_id, parent_id, selectors)
//     and returned it instead of creating ours. That entry is not ours, so it is
//     left alone rather than adopted or deleted.
//   - Result.Entry nil: our generated entry ID collided with an existing row.
//     The next cycle draws a fresh UUID.
func (c *Client) Create(ctx context.Context, entries []*types.Entry) (int, error) {
	var created int
	var errs []error

	for chunk := range chunks(entries, batchSize) {
		resp, err := c.api.BatchCreateEntry(ctx, &entryv1.BatchCreateEntryRequest{
			Entries:    chunk,
			OutputMask: managedFieldMask(),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("batch create: %w", err))
			continue
		}
		for i, res := range resp.Results {
			requested := ""
			if i < len(chunk) {
				requested = chunk[i].Id
			}
			switch code := codes.Code(res.GetStatus().GetCode()); {
			case code == codes.OK:
				created++
			case code == codes.AlreadyExists && res.Entry != nil && c.Owns(res.Entry):
				// Our own entry, from a cycle whose result had not reached the
				// entry list yet. Benign, and not worth a warning: the next
				// listing reconciles it away.
				c.log.Debug("this identity is already covered by an entry of ours",
					"requestedEntryID", requested, "existingEntryID", res.Entry.Id,
					"spiffeID", spiffeIDString(res.Entry.SpiffeId))
			case code == codes.AlreadyExists && res.Entry != nil && res.Entry.Id != requested:
				c.log.Warn("an entry outside this syncer's ownership already covers this identity; leaving it alone",
					"requestedEntryID", requested, "existingEntryID", res.Entry.Id,
					"spiffeID", spiffeIDString(res.Entry.SpiffeId))
			case code == codes.AlreadyExists:
				c.log.Warn("entry ID collision; a new ID will be generated on the next cycle",
					"entryID", requested)
			default:
				errs = append(errs, fmt.Errorf("create %s: %s: %s",
					requested, code, res.GetStatus().GetMessage()))
			}
		}
	}
	return created, joinErrors("spireentry: creating entries", errs)
}

// Update updates entries in place, returning the number actually updated.
func (c *Client) Update(ctx context.Context, entries []*types.Entry) (int, error) {
	var updated int
	var errs []error

	for chunk := range chunks(entries, batchSize) {
		resp, err := c.api.BatchUpdateEntry(ctx, &entryv1.BatchUpdateEntryRequest{
			Entries:    chunk,
			InputMask:  managedFieldMask(),
			OutputMask: managedFieldMask(),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("batch update: %w", err))
			continue
		}
		for i, res := range resp.Results {
			requested := ""
			if i < len(chunk) {
				requested = chunk[i].Id
			}
			switch code := codes.Code(res.GetStatus().GetCode()); code {
			case codes.OK:
				updated++
			case codes.NotFound:
				// Deleted underneath us; the next list picks up the new state.
				c.log.Warn("entry vanished before it could be updated", "entryID", requested)
			default:
				errs = append(errs, fmt.Errorf("update %s: %s: %s",
					requested, code, res.GetStatus().GetMessage()))
			}
		}
	}
	return updated, joinErrors("spireentry: updating entries", errs)
}

// Delete removes entries by ID, returning the number actually deleted.
// A NotFound result counts as success: the desired end state is reached either
// way.
func (c *Client) Delete(ctx context.Context, ids []string) (int, error) {
	var deleted int
	var errs []error

	for chunk := range chunks(ids, batchSize) {
		resp, err := c.api.BatchDeleteEntry(ctx, &entryv1.BatchDeleteEntryRequest{Ids: chunk})
		if err != nil {
			errs = append(errs, fmt.Errorf("batch delete: %w", err))
			continue
		}
		for _, res := range resp.Results {
			switch code := codes.Code(res.GetStatus().GetCode()); code {
			case codes.OK, codes.NotFound:
				deleted++
			default:
				errs = append(errs, fmt.Errorf("delete %s: %s: %s",
					res.GetId(), code, res.GetStatus().GetMessage()))
			}
		}
	}
	return deleted, joinErrors("spireentry: deleting entries", errs)
}

// chunks yields successive slices of at most size elements.
func chunks[T any](s []T, size int) func(func([]T) bool) {
	return func(yield func([]T) bool) {
		for start := 0; start < len(s); start += size {
			end := min(start+size, len(s))
			if !yield(s[start:end]) {
				return
			}
		}
	}
}

func joinErrors(context string, errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%s: %w", context, errors.Join(errs...))
}

func spiffeIDString(id *types.SPIFFEID) string {
	if id == nil {
		return ""
	}
	return "spiffe://" + id.TrustDomain + id.Path
}
