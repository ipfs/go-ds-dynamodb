package ddbds

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ipfs/go-datastore"
)

const (
	maxBatchChunkAttempts = 3
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
	var keys []datastore.Key
	for k := range b.reqs {
		keys = append(keys, k)
	}
	return b.commitKeys(ctx, keys)
}

func (b *batch) commitKeys(ctx context.Context, keys []datastore.Key) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	log.Debugf("committing batch", "Batch", keys)
	errs := make(chan error)
	chunks := chunk(len(keys), 25)
	for _, chunk := range chunks {
		var writeReqs []*dynamodb.WriteRequest
		for _, keyIdx := range chunk {
			k := keys[keyIdx]
			v := b.reqs[k]
			if v != nil {
				// put
				itemMap, err := b.ds.makePutItem(k, v, 0)
				if err != nil {
					return err
				}
				writeReqs = append(writeReqs, &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{Item: itemMap},
				})
			} else {
				// delete
				itemMap, err := b.ds.makeDeleteItemMap(k)
				if err != nil {
					return err
				}
				writeReqs = append(writeReqs, &dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{Key: itemMap},
				})
			}

		}
		go b.commitChunk(ctx, errs, writeReqs)
	}

	for i := 0; i < len(chunks); i++ {
		err := <-errs
		if err != nil {
			return err
		}
	}

	return nil

}

func (b *batch) commitChunk(ctx context.Context, errs chan<- error, chunk []*dynamodb.WriteRequest) {
	attempts := 0

	var err error

	defer func() {
		select {
		case errs <- err:
		case <-ctx.Done():
		}
	}()

	var res *dynamodb.BatchWriteItemOutput
	for attempts < maxBatchChunkAttempts {
		attempts++

		batchReq := dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{b.ds.table: chunk},
		}
		res, err = b.ds.ddbClient.BatchWriteItemWithContext(ctx, &batchReq)
		if err != nil {
			return
		}
		if len(res.UnprocessedItems[b.ds.table]) == 0 {
			return
		}

		chunk = res.UnprocessedItems[b.ds.table]

		// sleep using exponential backoff w/ jitter
		jitter := (b.ds.rand.Float64() * 0.2) + 0.9            // jitter factor is in interval [0.9:1.1]
		delayMS := math.Exp2(float64(attempts)) * 250 * jitter // delays are approx 500, 1000, 2000, 4000, ...

		delay := time.Duration(time.Duration(delayMS) * time.Millisecond)
		time.Sleep(delay)
	}

	err = fmt.Errorf("reached max attempts (%d) trying to commit batch to DynamoDB, last error: %w", maxBatchChunkAttempts, err)
}

// chunk returns a list of chunks, each consisting of a list of array indexes.
func chunk(len int, chunkSize int) [][]int {
	if chunkSize == 0 {
		return nil
	}
	var chunks [][]int
	for i := 0; i < len; i++ {
		chunkIdx := i / chunkSize
		elemIdx := i % chunkSize
		if elemIdx == 0 {
			chunks = append(chunks, nil)
		}
		chunks[chunkIdx] = append(chunks[chunkIdx], i)
	}
	return chunks
}
