package ddbds

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-datastore/query"
)

// scanIterator parallel scans a DynamoDB table for a query.
// The reader can control the consumed read capacity by controlling the rate at which Next() is invoked.
type scanIterator struct {
	ddbClient *dynamodb.Client
	tableName string
	indexName string
	segments  int
	keysOnly  bool

	doneWG sync.WaitGroup

	resultChan chan query.Result
	closeOnce  sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
}

func (s *scanIterator) trySend(result query.Result) bool {
	log.Debugw("sending scan result", "Result", result)
	select {
	case <-s.ctx.Done():
		return true
	case s.resultChan <- result:
	}
	return false
}

func (s *scanIterator) worker(segment int32, totalSegments int32) {
	defer log.Debug("scan worker done")
	log.Debug("scan worker starting")
	var exclusiveStartKey map[string]types.AttributeValue
	for {
		req := &dynamodb.ScanInput{
			TableName:         &s.tableName,
			Segment:           &segment,
			TotalSegments:     &totalSegments,
			ExclusiveStartKey: exclusiveStartKey,
		}

		if s.indexName != "" {
			req.IndexName = &s.indexName
		}

		if s.keysOnly {
			req.ProjectionExpression = aws.String(attrNameKey)
		}

		log.Debugw("scanning", "Req", req)
		res, err := s.ddbClient.Scan(s.ctx, req)
		if err != nil {
			s.trySend(query.Result{Error: err})
			return
		}
		for _, itemMap := range res.Items {
			log.Debugw("scan got items", "NumItems", len(res.Items))
			result := itemMapToQueryResult(itemMap, s.keysOnly)
			if s.trySend(result) {
				return
			}
		}
		if res.LastEvaluatedKey == nil {
			return
		}
		exclusiveStartKey = res.LastEvaluatedKey
	}
}

func itemMapToQueryResult(itemMap map[string]types.AttributeValue, keysOnly bool) query.Result {
	item, err := unmarshalItem(itemMap)
	if err != nil {
		return query.Result{Error: err}
	}
	result := query.Result{Entry: query.Entry{Key: item.DSKey}}
	if !keysOnly {
		result.Expiration = item.GetExpiration()
		result.Size = int(item.Size)
		result.Value = item.Value
	}
	return result
}

func (s *scanIterator) start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.resultChan = make(chan query.Result)
	totalSegments := int32(s.segments)
	for i := range s.segments {
		segment := int32(i)
		s.doneWG.Go(func() { s.worker(segment, totalSegments) })
	}
	// Don't wait on the Close() method to be called to close the chan;
	// close it as soon as there are no more results, so that Next() will return false.
	// If Close() is called, it races with this, hence the use of sync.Once.
	go func() {
		s.doneWG.Wait()
		s.closeOnce.Do(func() { close(s.resultChan) })
	}()
}

func (s *scanIterator) Next() (query.Result, bool) {
	result, ok := <-s.resultChan
	return result, ok
}

func (s *scanIterator) Close() error {
	s.cancel()
	s.doneWG.Wait()
	s.closeOnce.Do(func() { close(s.resultChan) })
	return nil
}
