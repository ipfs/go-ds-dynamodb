package ddbds

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-datastore"
)

const (
	maxBatchChunkAttempts = 3
	// dynamoBatchMaxItems is the BatchWriteItem hard limit imposed by
	// DynamoDB: at most 25 put-or-delete requests per call. See
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
	dynamoBatchMaxItems = 25
)

func (d *DDBDatastore) Batch(_ context.Context) (datastore.Batch, error) {
	return &batch{
		ds:   d,
		reqs: map[datastore.Key][]byte{},
	}, nil
}

type batch struct {
	ds *DDBDatastore

	// if the value exists but is nil, then it's a delete
	reqs map[datastore.Key][]byte
}

func (b *batch) Put(ctx context.Context, key datastore.Key, value []byte) error {
	b.reqs[key] = value
	return nil
}

func (b *batch) Delete(ctx context.Context, key datastore.Key) error {
	b.reqs[key] = nil
	return nil
}

func (b *batch) Commit(ctx context.Context) error {
	return b.commitKeys(ctx, slices.Collect(maps.Keys(b.reqs)))
}

func (b *batch) commitKeys(ctx context.Context, keys []datastore.Key) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	log.Debugw("committing batch", "Batch", keys)
	// Buffer errs so each goroutine's send always succeeds without a
	// receiver, even when ctx is cancelled. With an unbuffered channel
	// the deferred select below would race ctx.Done against the err
	// send and could exit silently, leaving the parent loop blocked on
	// <-errs forever.
	chunkCount := (len(keys) + dynamoBatchMaxItems - 1) / dynamoBatchMaxItems
	errs := make(chan error, chunkCount)
	for keyChunk := range slices.Chunk(keys, dynamoBatchMaxItems) {
		writeReqs := make([]types.WriteRequest, 0, len(keyChunk))
		for _, k := range keyChunk {
			v := b.reqs[k]
			if v != nil {
				// put
				itemMap, err := b.ds.makePutItem(k, v, 0)
				if err != nil {
					return err
				}
				writeReqs = append(writeReqs, types.WriteRequest{
					PutRequest: &types.PutRequest{Item: itemMap},
				})
			} else {
				// delete
				itemMap, err := b.ds.makeDeleteItemMap(k)
				if err != nil {
					return err
				}
				writeReqs = append(writeReqs, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: itemMap},
				})
			}
		}
		go b.commitChunk(ctx, errs, writeReqs)
	}

	for range chunkCount {
		if err := <-errs; err != nil {
			return err
		}
	}

	return nil
}

func (b *batch) commitChunk(ctx context.Context, errs chan<- error, chunk []types.WriteRequest) {
	attempts := 0

	var err error

	// errs is buffered to chunkCount in commitKeys, so this send never
	// blocks even if the parent has already returned.
	defer func() { errs <- err }()

	var res *dynamodb.BatchWriteItemOutput
	for attempts < maxBatchChunkAttempts {
		attempts++

		batchReq := dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{b.ds.table: chunk},
		}
		res, err = b.ds.ddbClient.BatchWriteItem(ctx, &batchReq)
		if err != nil {
			return
		}
		if len(res.UnprocessedItems[b.ds.table]) == 0 {
			return
		}

		chunk = res.UnprocessedItems[b.ds.table]

		// sleep using exponential backoff w/ jitter
		jitter := (rand.Float64() * 0.2) + 0.9                 // jitter factor is in interval [0.9:1.1]
		delayMS := math.Exp2(float64(attempts)) * 250 * jitter // delays are approx 500, 1000, 2000, 4000, ...

		delay := time.Duration(delayMS) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			err = ctx.Err()
			return
		}
	}

	// We exhausted retries while DynamoDB kept returning UnprocessedItems.
	// In the current control flow err is always nil here (any BatchWriteItem
	// failure returns early above), but the conditional keeps the error
	// message useful if the retry policy ever grows to retry on err too.
	if err != nil {
		err = fmt.Errorf("batch had unprocessed items after %d attempts, last error: %w", maxBatchChunkAttempts, err)
	} else {
		err = fmt.Errorf("batch had unprocessed items after %d attempts", maxBatchChunkAttempts)
	}
}
