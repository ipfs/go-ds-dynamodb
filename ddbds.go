package ddbds

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	golog "github.com/ipfs/go-log/v2"
)

const (
	attrNameKey        = "DSKey"
	attrNameSize       = "Size"
	attrNameExpiration = "Expiration"
)

var (
	log = golog.Logger("ddbds")

	ErrInvalidKey = errors.New("invalid key for DynamoDB datastore")
)

type Options struct {
	UseStronglyConsistentReads bool
	ScanParallelism            int
	PartitionKey               string
	SortKey                    string

	disableQueries bool
	disableScans   bool
}

func WithStronglyConsistentReads() func(o *Options) {
	return func(o *Options) {
		o.UseStronglyConsistentReads = true
	}
}

func WithScanParallelism(n int) func(o *Options) {
	return func(o *Options) {
		o.ScanParallelism = n
	}
}

func WithSortKey(sortKey string) func(o *Options) {
	return func(o *Options) {
		o.SortKey = sortKey
	}
}

func WithPartitionkey(partitionKey string) func(o *Options) {
	return func(o *Options) {
		o.PartitionKey = partitionKey
	}
}

func withDisableQueries() func(o *Options) {
	return func(o *Options) {
		o.disableQueries = true
	}
}

func withDisableScans() func(o *Options) {
	return func(o *Options) {
		o.disableScans = true
	}
}

var _ ds.Datastore = (*DDBDatastore)(nil)
var _ ds.Batching = (*DDBDatastore)(nil)
var _ ds.PersistentDatastore = (*DDBDatastore)(nil)

func New(ddbClient *dynamodb.DynamoDB, table string, optFns ...func(o *Options)) *DDBDatastore {
	opts := Options{}
	for _, o := range optFns {
		o(&opts)
	}

	ddbDS := &DDBDatastore{
		ddbClient:                  ddbClient,
		scanParallelism:            opts.ScanParallelism,
		useStronglyConsistentReads: opts.UseStronglyConsistentReads,
		table:                      table,
		partitionKey:               opts.PartitionKey,
		sortKey:                    opts.SortKey,
		disableQueries:             opts.disableQueries,
		disableScans:               opts.disableScans,
		rand:                       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	if ddbDS.scanParallelism == 0 {
		ddbDS.scanParallelism = 1
	}

	if ddbDS.partitionKey == "" {
		ddbDS.partitionKey = attrNameKey
	}

	return ddbDS
}

type DDBDatastore struct {
	ddbClient *dynamodb.DynamoDB
	table     string

	useStronglyConsistentReads bool

	partitionKey string

	// Optional, if not specified then scans are performed instead of queries.
	// If specified, then all keys must have at least 2 parts (for partition and sort keys).
	sortKey string

	// Controls the parallelism of scans that are preformed for unoptimized datastore queries.
	// Unoptimized datastore queries are queries without registered query prefixes, and always
	// result in full table scans.
	scanParallelism int

	disableQueries bool
	disableScans   bool

	rand *rand.Rand
}

var _ ds.Datastore = (*DDBDatastore)(nil)

// ddbItem is a raw DynamoDB item.
// Note that some attributes may not be present if a projection expression was used when reading the item.
type ddbItem struct {
	DSKey      string
	Value      []byte `dynamodbav:",omitempty"`
	Size       int64
	Expiration int64
}

func (d *ddbItem) GetExpiration() time.Time {
	return time.Unix(d.Expiration, 0)
}

func unmarshalItem(itemMap map[string]*dynamodb.AttributeValue) (*ddbItem, error) {
	item := &ddbItem{}
	err := dynamodbattribute.UnmarshalMap(itemMap, item)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling item: %w", err)
	}
	return item, nil
}

// getItem fetches an item from DynamoDB.
// If attributes is non-nil, only those attributes are fetched. This doesn't reduce consumed read capacity,
// it only reduces the amount of data transferred.
func (d *DDBDatastore) getItem(ctx context.Context, key ds.Key, attributes []string) (*ddbItem, error) {
	keyAttrs, ok := d.getKey(key)
	if !ok {
		return nil, ErrInvalidKey
	}

	req := &dynamodb.GetItemInput{
		TableName:      &d.table,
		Key:            keyAttrs,
		ConsistentRead: &d.useStronglyConsistentReads,
	}

	if attributes != nil {
		projExprStrs := []string{}
		projExprNames := map[string]*string{}
		for i, attr := range attributes {
			expr := fmt.Sprintf("#k%d", i)
			projExprNames[expr] = aws.String(attr)
			projExprStrs = append(projExprStrs, expr)
		}
		req.ProjectionExpression = aws.String(strings.Join(projExprStrs, ","))
		req.ExpressionAttributeNames = projExprNames
	}

	res, err := d.ddbClient.GetItemWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, ds.ErrNotFound
	}
	return unmarshalItem(res.Item)
}

func (d *DDBDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	item, err := d.getItem(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (d *DDBDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	keyAttrs, ok := d.getKey(key)
	if !ok {
		return false, ErrInvalidKey
	}

	res, err := d.ddbClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:                &d.table,
		Key:                      keyAttrs,
		ProjectionExpression:     aws.String("#k"),
		ExpressionAttributeNames: map[string]*string{"#k": &d.partitionKey},
	})
	if err != nil {
		return false, err
	}
	return res.Item != nil, nil
}

func (d *DDBDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	item, err := d.getItem(ctx, key, []string{attrNameSize})
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return -1, err
		}
		return 0, err
	}
	return int(item.Size), nil
}

func (d *DDBDatastore) makePutItem(key ds.Key, value []byte, ttl time.Duration) (map[string]*dynamodb.AttributeValue, error) {
	keyAttrs, ok := d.putKey(key)
	if !ok {
		return nil, ErrInvalidKey
	}

	item := &ddbItem{
		Size:  int64(len(value)),
		Value: value,
	}

	if ttl > 0 {
		item.Expiration = time.Now().Add(ttl).Unix()
	}

	itemMap, err := dynamodbattribute.MarshalMap(*item)
	if err != nil {
		return nil, fmt.Errorf("marshaling item: %w", err)
	}

	for k, v := range keyAttrs {
		itemMap[k] = v
	}
	return itemMap, nil
}

func (d *DDBDatastore) put(ctx context.Context, key ds.Key, value []byte, ttl time.Duration) error {
	itemMap, err := d.makePutItem(key, value, ttl)
	if err != nil {
		return err
	}

	req := &dynamodb.PutItemInput{
		TableName: &d.table,
		Item:      itemMap,
	}

	log.Debugw("putting items", "Item", req)

	_, err = d.ddbClient.PutItemWithContext(ctx, req)
	log.Debug("done putting items")
	if err != nil {
		return fmt.Errorf("putting DynamoDB item '%s' into table '%s': %w", key.String(), d.table, err)
	}

	return nil
}

func (d *DDBDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	return d.put(ctx, key, value, time.Duration(0))
}

func (d *DDBDatastore) makeDeleteItemMap(key ds.Key) (map[string]*dynamodb.AttributeValue, error) {
	keyAttrs, ok := d.getKey(key)
	if !ok {
		return nil, ErrInvalidKey
	}
	return keyAttrs, nil
}

func (d *DDBDatastore) Delete(ctx context.Context, key ds.Key) error {
	itemMap, err := d.makeDeleteItemMap(key)
	if err != nil {
		return err
	}
	req := &dynamodb.DeleteItemInput{
		TableName: &d.table,
		Key:       itemMap,
	}

	_, err = d.ddbClient.DeleteItemWithContext(ctx, req)
	if err != nil {
		// note that DeleteItem is idempotent in that deleting an non-existent item returns a 200 w/ no error
		// which is congruent with the Datastore interface
		return err
	}
	return nil
}

func (d *DDBDatastore) Sync(ctx context.Context, prefix ds.Key) error { return nil }

func (d *DDBDatastore) Close() error { return nil }

func (d *DDBDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	useNaiveOrders := len(q.Orders) > 0

	var results query.Results

	prefix := ds.NewKey(q.Prefix)
	keyAttrs, hasQueryKey := d.queryKey(prefix)

	shouldQuery := d.sortKey != "" && hasQueryKey
	if shouldQuery {
		log.Debugw("querying", "Table", d.table, "Key", keyAttrs)
		if d.disableQueries {
			return nil, fmt.Errorf("queries on '%s' are disabled", d.table)
		}

		partitionKeyValue := *keyAttrs[d.partitionKey].S
		ddbQuery := &dynamodb.QueryInput{
			TableName:                 &d.table,
			KeyConditionExpression:    aws.String("#k = :v"),
			ExpressionAttributeNames:  map[string]*string{"#k": &d.partitionKey},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":v": {S: &partitionKeyValue}},
			ConsistentRead:            &d.useStronglyConsistentReads,
		}

		// we can only do this with queries and if there is exactly one order,
		// otherwise we have to do the sorting client-side
		if len(q.Orders) == 1 {
			if _, ok := q.Orders[0].(*query.OrderByKeyDescending); ok {
				ddbQuery.ScanIndexForward = aws.Bool(false)
				useNaiveOrders = false
			}
		}

		queryIter := newQueryIterator(d.ddbClient, ddbQuery, q.KeysOnly)
		queryIter.start(ctx)
		results = query.ResultsFromIterator(q, query.Iterator{
			Next:  queryIter.Next,
			Close: queryIter.Close,
		})
	} else {
		log.Debugw("scanning", "Table", d.table, "Key", keyAttrs)
		if d.disableScans {
			return nil, fmt.Errorf("scans on '%s' are disabled", d.table)
		}

		scanIter := &scanIterator{
			ddbClient: d.ddbClient,
			tableName: d.table,
			segments:  d.scanParallelism,
			keysOnly:  q.KeysOnly,
		}
		scanIter.start(ctx)
		results = query.ResultsFromIterator(q, query.Iterator{
			Next:  scanIter.Next,
			Close: scanIter.Close,
		})
	}

	// append / so a prefix of /bar only finds /bar/baz, not /barbaz
	queryPrefix := prefix.String()
	if len(queryPrefix) > 0 && queryPrefix[len(queryPrefix)-1] != '/' {
		queryPrefix += "/"
	}
	log.Debugw("using query prefix", "Prefix", queryPrefix)
	results = query.NaiveFilter(results, query.FilterKeyPrefix{Prefix: queryPrefix})

	// TODO: some kinds of filters can be done server-side
	for _, f := range q.Filters {
		results = query.NaiveFilter(results, f)
	}

	if useNaiveOrders {
		results = query.NaiveOrder(results, q.Orders...)
	}

	// this is not possible to do server-side with DynamoDB
	if q.Offset != 0 {
		results = query.NaiveOffset(results, q.Offset)
	}

	// TODO: this will usually over-read the last page...not terrible, but we can do better
	if q.Limit != 0 {
		results = query.NaiveLimit(results, q.Limit)
	}

	return results, nil
}

func (d *DDBDatastore) PutWithTTL(ctx context.Context, key ds.Key, value []byte, ttl time.Duration) error {
	return d.put(ctx, key, value, ttl)
}

func (d *DDBDatastore) SetTTL(ctx context.Context, key ds.Key, ttl time.Duration) error {
	expiration := time.Now().Add(ttl).Unix()
	expirationStr := strconv.Itoa(int(expiration))
	keyAttrs, ok := d.putKey(key)
	if !ok {
		return ErrInvalidKey
	}

	req := &dynamodb.UpdateItemInput{
		TableName:        &d.table,
		Key:              keyAttrs,
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :e", attrNameExpiration)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":e": {N: &expirationStr},
		},
		ConditionExpression:      aws.String("attribute_exists(#k)"),
		ExpressionAttributeNames: map[string]*string{"#k": aws.String(attrNameKey)},
	}

	log.Debugw("updating TTL", "Item", req)

	_, err := d.ddbClient.UpdateItemWithContext(ctx, req)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// the conditional check failed which means there is no such item to set the TTL on
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return ds.ErrNotFound
			}
		}
		return fmt.Errorf("setting TTL DynamoDB item to table '%s': %w", d.table, err)
	}
	return nil
}
func (d *DDBDatastore) GetExpiration(ctx context.Context, key ds.Key) (time.Time, error) {
	item, err := d.getItem(ctx, key, []string{attrNameExpiration})
	if err != nil {
		return time.Time{}, err
	}
	return item.GetExpiration(), nil
}

// DiskUsage returns the size of the DynamoDB table.
// Note that DynamoDB only updates this size once every few hours.
// The underlying call is heavily throttled so this should only be called occasionally.
func (d *DDBDatastore) DiskUsage(ctx context.Context) (uint64, error) {
	res, err := d.ddbClient.DescribeTable(&dynamodb.DescribeTableInput{TableName: &d.table})
	if err != nil {
		return 0, err
	}
	return uint64(*res.Table.TableSizeBytes), nil
}

func (d *DDBDatastore) EntryCount(ctx context.Context) (uint64, error) {
	res, err := d.ddbClient.DescribeTable(&dynamodb.DescribeTableInput{TableName: &d.table})
	if err != nil {
		return 0, err
	}
	return uint64(*res.Table.ItemCount), nil
}
