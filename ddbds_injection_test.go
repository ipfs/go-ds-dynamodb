package ddbds

// Tests in this file drive ddbds through middleware-injected failure
// modes that DynamoDB Local cannot produce on demand: Scan returning
// (nil, err), BatchWriteItem returning UnprocessedItems on every call,
// and so on. They run alongside the integration tests but do not
// require DynamoDB Local to be reachable on the listed endpoint; the
// middleware short-circuits every API call before the HTTP layer runs.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
)

// TestForcedErrorPropagates is a smoke test for forceSDKErrorMiddleware:
// an injected error must reach the caller as a normal SDK error. Every
// other middleware-based test in this file relies on this contract.
func TestForcedErrorPropagates(t *testing.T) {
	injected := errors.New("injected error")
	client := newDDBClient(clientOpts{
		endpoint:   fmt.Sprintf("http://%s", ddbLocalEndpoint),
		forceError: injected,
	})

	_, err := client.ListTables(t.Context(), &dynamodb.ListTablesInput{})
	require.ErrorIs(t, err, injected)
}

// TestScanWorkerHandlesScanError is the direct regression test for the
// v2 migration's headline bug fix: when Scan returns (nil, err), the
// worker must surface the error and exit without dereferencing
// res.Items. DDB Local cannot fail a well-formed scan on demand, so we
// inject the error through the smithy middleware stack.
func TestScanWorkerHandlesScanError(t *testing.T) {
	injected := errors.New("simulated scan failure")
	client := newDDBClient(clientOpts{
		endpoint:   fmt.Sprintf("http://%s", ddbLocalEndpoint),
		forceError: injected,
	})
	// No sort key configured, so Query with no prefix takes the scan
	// path. The injected error fires on Scan before any HTTP work.
	dsi := New(client, "no-such-table", WithPartitionkey("key"))

	res, err := dsi.Query(t.Context(), query.Query{})
	require.NoError(t, err)
	defer res.Close()

	r, ok := res.NextSync()
	require.True(t, ok, "expected the scan worker to forward the error")
	require.ErrorIs(t, r.Error, injected)
}

// TestCommitRespectsContextCancellation verifies that commitChunk's
// retry backoff honours ctx cancellation. With every BatchWriteItem
// response flagged as UnprocessedItems, the goroutine enters the
// exponential backoff sleep; cancelling ctx mid-sleep must return
// promptly with ctx.Err rather than waiting out the full ~3.5 s
// backoff sequence.
func TestCommitRespectsContextCancellation(t *testing.T) {
	client := newDDBClient(clientOpts{
		endpoint:   fmt.Sprintf("http://%s", ddbLocalEndpoint),
		apiOptions: []func(*middleware.Stack) error{alwaysUnprocessedItemsMiddleware()},
	})
	dsi := New(client, "no-such-table", WithPartitionkey("key"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	b, err := dsi.Batch(ctx)
	require.NoError(t, err)
	require.NoError(t, b.Put(ctx, ds.NewKey("/k"), []byte("v")))

	// Fire cancellation during the first backoff (~500 ms). Without the
	// ctx-aware sleep, commitChunk would block the whole ~3.5 s backoff
	// sequence before noticing the cancellation.
	const cancelAfter = 50 * time.Millisecond
	const commitDeadline = 400 * time.Millisecond
	go func() {
		time.Sleep(cancelAfter)
		cancel()
	}()

	start := time.Now()
	err = b.Commit(ctx)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, commitDeadline,
		"commit blocked %s waiting for the full backoff", elapsed)
}

// alwaysUnprocessedItemsMiddleware short-circuits every BatchWriteItem
// call with a response that echoes the request back as UnprocessedItems.
// commitChunk's retry loop then enters the backoff on every attempt,
// giving tests a deterministic way to reach the backoff path without
// depending on real DynamoDB throttling.
func alwaysUnprocessedItemsMiddleware() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc("alwaysUnprocessedItems",
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
				) (middleware.InitializeOutput, middleware.Metadata, error) {
					bwi, ok := in.Parameters.(*dynamodb.BatchWriteItemInput)
					if !ok {
						return next.HandleInitialize(ctx, in)
					}
					return middleware.InitializeOutput{
						Result: &dynamodb.BatchWriteItemOutput{
							UnprocessedItems: maps.Clone(bwi.RequestItems),
						},
					}, middleware.Metadata{}, nil
				}),
			middleware.Before,
		)
	}
}
