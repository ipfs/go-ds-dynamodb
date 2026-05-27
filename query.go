package ddbds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ipfs/go-datastore/query"
)

// queryIterator queries a DynamoDB table/index for a query.
// Queries cannot be performed in parallel, they are paginated sequentially.
type queryIterator struct {
	ddbClient *dynamodb.Client
	cancel    context.CancelFunc
	ddbQuery  *dynamodb.QueryInput

	keysOnly bool

	resultChan chan query.Result
}

func newQueryIterator(ddbClient *dynamodb.Client, ddbQuery *dynamodb.QueryInput, keysOnly bool) *queryIterator {
	qi := &queryIterator{
		ddbClient:  ddbClient,
		ddbQuery:   ddbQuery,
		keysOnly:   keysOnly,
		resultChan: make(chan query.Result),
	}
	return qi
}

func (q *queryIterator) start(ctx context.Context) {
	ctx, q.cancel = context.WithCancel(ctx)
	go func() {
		defer close(q.resultChan)
		defer q.cancel()

		paginator := dynamodb.NewQueryPaginator(q.ddbClient, q.ddbQuery)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				trySend(ctx, q.resultChan, query.Result{Error: err})
				return
			}
			for _, itemMap := range page.Items {
				result := itemMapToQueryResult(itemMap, q.keysOnly)
				if !trySend(ctx, q.resultChan, result) {
					return
				}
			}
		}
	}()
}

func (q *queryIterator) Next() (query.Result, bool) {
	res, ok := <-q.resultChan
	return res, ok
}

func (q *queryIterator) Close() error {
	q.cancel()
	return nil
}
