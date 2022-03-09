package ddbds

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	golog "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ddbLocalDownloadPath    = "s3.us-west-2.amazonaws.com/dynamodb-local"
	ddbLocalArchiveFilename = "dynamodb_local_2022-01-10.tar.gz"
	ddbLocalDir             = "ddblocal"

	tableName        = "testtable"
	ddbLocalEndpoint = "127.0.0.1:8000"

	logLevel = golog.LevelInfo

	ddbClient *dynamodb.DynamoDB
)

func init() {
	golog.SetAllLoggers(logLevel)
}

func TestMain(m *testing.M) {
	ddbClient = newDDBClient(clientOpts{endpoint: fmt.Sprintf("http://%s", ddbLocalEndpoint)})
	stopDDBLocal, err := startDDBLocal(context.Background(), ddbClient)
	if err != nil {
		panic(err)
	}
	exitCode := m.Run()
	stopDDBLocal()
	os.Exit(exitCode)
}

func downloadDDBLocal(ctx context.Context) error {
	if _, err := os.Stat(ddbLocalDir); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(ddbLocalDir, 0755)
		if err != nil {
			return err
		}
		dlPath := path.Join(ddbLocalDownloadPath, ddbLocalArchiveFilename)
		url := "https://" + dlPath
		fmt.Printf("fetching DynamoDB Local from %s\n", url)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("received status code %d when downloading DDB Local\n", resp.StatusCode)
		}

		uncompressed, err := gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		tarReader := tar.NewReader(uncompressed)
		basePath := ddbLocalDir
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			switch header.Typeflag {
			case tar.TypeDir:
				err = os.Mkdir(path.Join(basePath, header.Name), 0755)
				if err != nil {
					return err
				}
			case tar.TypeReg:
				f, err := os.Create(path.Join(basePath, header.Name))
				if err != nil {
					return err
				}
				_, err = io.Copy(f, tarReader)
				if err != nil {
					f.Close()
					return err
				}
				f.Close()
			default:
				return fmt.Errorf("unknown tar type '%x'", header.Typeflag)
			}
		}
	}
	return nil
}

func startDDBLocal(ctx context.Context, ddbClient *dynamodb.DynamoDB) (func(), error) {
	var cleanupFunc func()

	// in CI, run DynamoDB Local directly with Java
	// otherwise we use Docker for convenience
	if os.Getenv("CI") != "" {
		err := downloadDDBLocal(ctx)
		if err != nil {
			return nil, err
		}
		cmd := exec.Command(
			"java",
			fmt.Sprintf("-Djava.library.path=./%s/DynamoDBLocal_lib", ddbLocalDir),
			"-jar",
			fmt.Sprintf("./%s/DynamoDBLocal.jar", ddbLocalDir),
			"-inMemory",
		)
		err = cmd.Start()
		if err != nil {
			return nil, err
		}
		cleanupFunc = func() {
			fmt.Printf("killing %d\n", cmd.Process.Pid)
			err := cmd.Process.Kill()
			if err != nil {
				fmt.Printf("error killing %d: %s\n", cmd.Process.Pid, err)
			}
		}
	} else {
		cmd := exec.Command("docker", "run", "-d", "-p", "8000:8000", "amazon/dynamodb-local", "-jar", "DynamoDBLocal.jar", "-inMemory")
		buf := &bytes.Buffer{}
		cmd.Stdout = buf
		cmd.Stderr = buf
		err := cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("error running DynamoDB Local (%s), output:\n%s", err.Error(), buf)
		}

		fmt.Printf("Ran DynamoDB Local container ")

		ctrID := strings.TrimSpace(buf.String())

		cleanupFunc = func() {
			fmt.Printf("killing %s\n", ctrID)
			cmd := exec.Command("docker", "kill", ctrID)
			if err := cmd.Run(); err != nil {
				fmt.Printf("error killing %s: %s\n", ctrID, err)
			}
		}
	}

	// wait for DynamoDB to respond
	for {
		select {
		case <-ctx.Done():
			cleanupFunc()
			return nil, ctx.Err()
		default:
		}
		_, err := ddbClient.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{})
		if err == nil {
			break
		}
	}

	return cleanupFunc, nil
}

func forceSDKError(err error) func(*request.Request) {
	return func(r *request.Request) {
		r.Error = err
		r.Retryable = aws.Bool(false)
	}
}

type clientOpts struct {
	endpoint   string
	forceError error
}

func newDDBClient(opts clientOpts) *dynamodb.DynamoDB {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentials("a", "a", "a"),
		DisableSSL:  aws.Bool(true),
		Region:      aws.String(endpoints.UsEast1RegionID),
		Endpoint:    &opts.endpoint,
	}
	sess := session.Must(session.NewSession(cfg))
	if opts.forceError != nil {
		sess.Handlers.Send.PushFront(forceSDKError(opts.forceError))
	}
	return dynamodb.New(sess)
}

type table struct {
	name         string
	partitionKey string
	sortKey      string
}

func setupTables(ddbClient *dynamodb.DynamoDB, tables ...table) {
	for _, table := range tables {
		tbl := table

		attrDefs := []*dynamodb.AttributeDefinition{
			{AttributeName: &tbl.partitionKey, AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
		}
		keySchema := []*dynamodb.KeySchemaElement{
			{AttributeName: &tbl.partitionKey, KeyType: aws.String(dynamodb.KeyTypeHash)},
		}
		if tbl.sortKey != "" {
			attrDefs = append(attrDefs, &dynamodb.AttributeDefinition{AttributeName: &tbl.sortKey, AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)})
			keySchema = append(keySchema, &dynamodb.KeySchemaElement{AttributeName: &tbl.sortKey, KeyType: aws.String(dynamodb.KeyTypeRange)})
		}

		req := &dynamodb.CreateTableInput{
			AttributeDefinitions: attrDefs,
			KeySchema:            keySchema,
			TableName:            &tbl.name,
			BillingMode:          aws.String(dynamodb.BillingModeProvisioned),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1000),
				WriteCapacityUnits: aws.Int64(1000),
			},
		}

		log.Debugw("creating table", "Table", tbl.name, "Req", req)
		_, err := ddbClient.CreateTable(req)
		if err != nil {
			// idempotency
			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
				return
			}
			panic(err)
		}
	}
}

func cleanupTables(ddbClient *dynamodb.DynamoDB, tables ...table) {
	for _, t := range tables {
		log.Debugw("deleting table", "Table", t.name)
		_, err := ddbClient.DeleteTable(&dynamodb.DeleteTableInput{TableName: &t.name})
		if err != nil {
			panic(err)
		}
	}
}

func TestDDBDatastore_Batch(t *testing.T) {
	type batchOp struct {
		key      string
		value    string
		isDelete bool
	}

	type batch struct {
		forceCommitErr bool
		ops            []batchOp
	}

	type batchErrs struct {
		commitErr string
	}

	type entry struct {
		key   string
		value string
		err   string
	}

	makeBatch := func(start, n int) batch {
		b := batch{}
		for i := start; i < n; i++ {
			b.ops = append(b.ops, batchOp{key: "key-%06d", value: "val-%06d"})
		}
		return b
	}

	makeDeleteBatch := func(start, n int) batch {
		b := batch{}
		for i := start; i < n; i++ {
			b.ops = append(b.ops, batchOp{key: "key-%06d", isDelete: true})
		}
		return b
	}

	makeExpEntries := func(start, n int) []entry {
		var entries []entry
		for i := start; i < n; i++ {
			entries = append(entries, entry{key: "key-%06d", value: "val-%06d"})
		}
		return entries
	}

	concatEntries := func(entries ...[]entry) []entry {
		var es []entry
		for _, e := range entries {
			es = append(es, e...)
		}
		return es
	}

	cases := []struct {
		name    string
		batches []batch

		expBatchErrs []batchErrs
		expEntries   []entry
	}{
		{
			name:       "1 entry",
			batches:    []batch{{ops: []batchOp{{key: "/foo", value: "bar"}}}},
			expEntries: []entry{{key: "/foo", value: "bar"}},
		},
		{
			name: "multiple batches",
			batches: []batch{
				{ops: []batchOp{{key: "/a", value: "a-val"}}},
				{ops: []batchOp{{key: "/b", value: "b-val"}}},
				{ops: []batchOp{{key: "/c", value: "c-val"}}},
			},
			expEntries: []entry{
				{key: "/a", value: "a-val"},
				{key: "/b", value: "b-val"},
				{key: "/c", value: "c-val"},
			},
		},
		{
			name:       "one large batch that exceeds DynamoDB batch limit",
			batches:    []batch{makeBatch(0, 100)},
			expEntries: makeExpEntries(0, 100),
		},
		{
			name: "multiple large batches that exceed DynamoDB batch limit",
			batches: []batch{
				makeBatch(0, 100),
				makeBatch(100, 100),
			},
			expEntries: makeExpEntries(0, 200),
		},
		{
			name:       "batch just under batch limit",
			batches:    []batch{makeBatch(0, 24)},
			expEntries: makeExpEntries(0, 24),
		},
		{
			name:       "batch just over batch limit",
			batches:    []batch{makeBatch(0, 26)},
			expEntries: makeExpEntries(0, 26),
		},
		{
			name:       "batch exactly equaling batch limit",
			batches:    []batch{makeBatch(0, 25)},
			expEntries: makeExpEntries(0, 25),
		},
		{
			name:    "empty batch",
			batches: []batch{},
		},
		{
			name: "batch deletes work",
			batches: []batch{
				makeBatch(0, 100),
				makeDeleteBatch(80, 20),
				makeBatch(100, 10),
			},
			expEntries: concatEntries(makeExpEntries(0, 80), makeExpEntries(100, 109)),
		},
		{
			name: "returns an error on commit error",
			batches: []batch{
				{
					ops:            []batchOp{{key: "/a", value: "a-val"}},
					forceCommitErr: true,
				},
			},
			expBatchErrs: []batchErrs{{commitErr: "ResourceNotFoundException"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			tbl := table{name: tableName, partitionKey: "key"}
			setupTables(ddbClient, tbl)
			defer cleanupTables(ddbClient, tbl)

			ddbDS := DDBDatastore{
				ddbClient:    ddbClient,
				table:        tableName,
				partitionKey: "key",
			}

			for batchIdx, batch := range c.batches {
				func() {
					b, err := ddbDS.Batch(ctx)
					require.NoError(t, err)
					for _, op := range batch.ops {
						if op.isDelete {
							err := b.Delete(ctx, ds.NewKey(op.key))
							require.NoError(t, err)
						} else {
							err := b.Put(ctx, ds.NewKey(op.key), []byte(op.value))
							require.NoError(t, err)
						}
					}
					if batch.forceCommitErr {
						originalTable := ddbDS.table
						ddbDS.table = "non-existent-table"
						defer func() { ddbDS.table = originalTable }()
					}
					err = b.Commit(ctx)
					if c.expBatchErrs != nil && c.expBatchErrs[batchIdx].commitErr != "" {
						assert.Error(t, err)
						assert.Contains(t, err.Error(), c.expBatchErrs[batchIdx].commitErr)
						return
					}
					require.NoError(t, err)
				}()
			}

			for _, expEntry := range c.expEntries {
				val, err := ddbDS.Get(ctx, ds.NewKey(expEntry.key))
				if expEntry.err != "" {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), expEntry.err)
					continue
				}
				require.NoError(t, err)
				assert.EqualValues(t, expEntry.value, val)
			}
		})
	}
}

func TestDDBDatastore_DiskUsage(t *testing.T) {
	tbl := table{name: tableName, partitionKey: "key"}
	setupTables(ddbClient, tbl)
	defer cleanupTables(ddbClient, tbl)

	ddbDS := &DDBDatastore{
		ddbClient:    ddbClient,
		table:        tableName,
		partitionKey: "key",
	}

	ctx := context.Background()

	t.Run("no items should have size == 0", func(t *testing.T) {
		usage, err := ddbDS.DiskUsage(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 0, usage)
	})

	t.Run("one item should have a size > 0", func(t *testing.T) {
		err := ddbDS.Put(ctx, ds.NewKey("k1"), []byte("v1"))
		require.NoError(t, err)
		usage, err := ddbDS.DiskUsage(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 0x26, usage)
	})

	t.Run("returns an error when there's an underlying DDB client error", func(t *testing.T) {
		ddbDS.table = "non-existent-table"
		_, err := ddbDS.DiskUsage(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ResourceNotFoundException")
	})
}

func TestDDBDatastore_EntryCount(t *testing.T) {
	tbl := table{name: tableName, partitionKey: "key"}
	setupTables(ddbClient, tbl)
	defer cleanupTables(ddbClient, tbl)

	ddbDS := &DDBDatastore{
		ddbClient:    ddbClient,
		table:        tableName,
		partitionKey: "key",
	}

	ctx := context.Background()

	t.Run("no items should have size == 0", func(t *testing.T) {
		usage, err := ddbDS.EntryCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 0, usage)
	})

	t.Run("one item should have a size == 1", func(t *testing.T) {
		err := ddbDS.Put(ctx, ds.NewKey("k1"), []byte("v1"))
		require.NoError(t, err)
		usage, err := ddbDS.EntryCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, usage)
	})

	t.Run("returns an error when there's an underlying DDB client error", func(t *testing.T) {
		ddbDS.table = "non-existent-table"
		_, err := ddbDS.EntryCount(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ResourceNotFoundException")
	})
}

func TestDDBDatastore_PutAndGet(t *testing.T) {
	ddbSizedValue := []byte("bar")

	cases := []struct {
		name   string
		putKey string
		getKey string
		value  []byte

		expectPutErrContains string
		expectGetErrContains string
		expectGetValue       []byte
	}{
		{
			name:   "happy case",
			putKey: "foo",
			getKey: "foo",
			value:  ddbSizedValue,

			expectGetValue: ddbSizedValue,
		},
		{
			name:                 "returns ErrNotFound if the value is not in DDB",
			putKey:               "foo",
			getKey:               "foo1",
			expectGetErrContains: "datastore: key not found",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			tbl := table{name: tableName, partitionKey: "key"}
			setupTables(ddbClient, tbl)
			defer cleanupTables(ddbClient, tbl)

			ddbDS := &DDBDatastore{
				ddbClient:      ddbClient,
				table:          tableName,
				partitionKey:   "key",
				disableQueries: true,
				disableScans:   true,
			}

			err := ddbDS.Put(ctx, ds.NewKey(c.putKey), c.value)
			if c.expectPutErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.expectPutErrContains)
				return
			}
			require.NoError(t, err)

			val, err := ddbDS.Get(ctx, ds.NewKey(c.getKey))
			if c.expectGetErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.expectGetErrContains)
				return
			}
			require.NoError(t, err)

			require.Equal(t, len(c.expectGetValue), len(val)) // try to prevent printing huge values
			require.Equal(t, c.expectGetValue, val)
		})
	}
}

func TestDDBDatastore_Query(t *testing.T) {
	type queryResult struct {
		entry query.Entry
		err   string
	}

	orderByKey := []query.Order{&query.OrderByKey{}}

	makeEntries := func(keyPrefix string, n int) map[string]string {
		m := map[string]string{}
		for i := 0; i < n; i++ {
			m[fmt.Sprintf("%s%06d", keyPrefix, i)] = "val"
		}
		return m
	}

	makeExpResult := func(keyPrefix string, n int) []queryResult {
		var results []queryResult
		for i := 0; i < n; i++ {
			results = append(results, queryResult{entry: query.Entry{
				Key:   fmt.Sprintf("%s%06d", keyPrefix, i),
				Value: []byte("val"),
			}})
		}
		return results
	}

	cases := []struct {
		name             string
		overrideLogLevel *golog.LogLevel
		table            table
		ddbDS            *DDBDatastore
		dsEntries        map[string]string
		queries          []query.Query

		expResults     [][]queryResult
		expQueryErrors []string
		expPutError    string
	}{
		{
			name:             "100 query entries",
			overrideLogLevel: &golog.LevelInfo,
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				WithSortKey("table1SortKey"),
				withDisableScans(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries:  makeEntries("/a/b", 100),
			queries:    []query.Query{{Prefix: "/a", Orders: orderByKey}},
			expResults: [][]queryResult{makeExpResult("/a/b", 100)},
		},
		{
			name:             "100 scan entries",
			overrideLogLevel: &golog.LevelInfo,
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				withDisableQueries(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			},
			dsEntries:  makeEntries("/a", 100),
			queries:    []query.Query{{Orders: orderByKey}},
			expResults: [][]queryResult{makeExpResult("/a", 100)},
		},
		{
			name: "prefix query",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				WithSortKey("table1SortKey"),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries: map[string]string{
				"/foo/bar/baz":     "qux",
				"/foo/bar/baz/qux": "quux",
			},
			queries: []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expResults: [][]queryResult{{
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("qux")}},
				{entry: query.Entry{Key: "/foo/bar/baz/qux", Value: []byte("quux")}},
			}},
		},
		{
			// TODO: can this tell that sort happened at ddb layer?
			name: "prefix query with dynamodb-optimized descending order",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				WithSortKey("table1SortKey"),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries: map[string]string{
				"/foo/z/a": "bar",
				"/foo/z/c": "bar",
				"/foo/z/b": "bar",

				"/foo/x/a": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo/z",
				Orders: []query.Order{&query.OrderByKeyDescending{}},
			}},
			expResults: [][]queryResult{{
				{entry: query.Entry{Key: "/foo/z/c", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/z/b", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/z/a", Value: []byte("bar")}},
			}},
		},
		{
			name: "prefix query with naive filters and orders, and a non-matching first transform",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				WithSortKey("table1SortKey"),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries: map[string]string{
				"/foo/k/a": "bar1",
				"/foo/k/c": "bar3",
				"/foo/k/b": "bar2",
				"/foo/k/e": "bar5",
				"/foo/k/d": "bar4",
				"/foo/k/f": "bar6",
				"/foo/k/g": "bar7",
				"/foo/k/h": "bar8",

				"/qux/k/quux": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo/k",
				Orders: []query.Order{&query.OrderByKeyDescending{}, &query.OrderByValue{}},
				Limit:  2,
				Offset: 1,
				Filters: []query.Filter{&query.FilterKeyCompare{
					Op:  query.GreaterThan,
					Key: "/foo/k/c",
				}},
			}},
			expResults: [][]queryResult{{
				{entry: query.Entry{Key: "/foo/k/g", Value: []byte("bar7")}},
				{entry: query.Entry{Key: "/foo/k/f", Value: []byte("bar6")}},
			}},
		},
		{
			name: "root prefix /",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				withDisableQueries(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			},
			dsEntries: map[string]string{
				"/foo":              "bar",
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",
				"/quux":             "quuz",
			},
			queries: []query.Query{{Prefix: "/", Orders: orderByKey}},
			expResults: [][]queryResult{{
				{entry: query.Entry{Key: "/foo", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				{entry: query.Entry{Key: "/foo/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/quux", Value: []byte("quuz")}},
			}},
		},
		{
			name: "scanning table",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				withDisableQueries(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			},
			dsEntries: map[string]string{
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/bar/baz/bang": "boom",
			},
			queries: []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expResults: [][]queryResult{{
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
			}},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when scans are disabled and a query requires a scan",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				withDisableScans(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			},
			dsEntries:      map[string]string{"/foo/bar/baz": "bang"},
			queries:        []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expQueryErrors: []string{"scans on 'table1' are disabled"},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when queries are disabled and a query requires a query",
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
				WithSortKey("table1SortKey"),
				withDisableQueries(),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries:      map[string]string{"/foo/bar/baz": "bang"},
			queries:        []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expQueryErrors: []string{"queries on 'table1' are disabled"},
		},
		{
			name: "returns an error if the datastore is missing the table's sort key",
			// Note that the inverse isn't true, if the table is missing the datastore's sort key
			// then we have no idea because DynamoDB will still happily accept the "sort" key
			// as just another attribute, and then the item will only appear in scans.
			//
			// There's not much we can do about this, perhaps describe the tables when the datastore
			// starts up and verify that they their schema matches the key transforms?
			ddbDS: New(
				ddbClient,
				"table1",
				WithPartitionkey("table1PartitionKey"),
			),
			table: table{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			},
			dsEntries:   map[string]string{"/foo/bar/baz": "bang"},
			expPutError: "One of the required keys was not given a value",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()

			if c.overrideLogLevel != nil {
				golog.SetAllLoggers(*c.overrideLogLevel)
				defer golog.SetAllLoggers(logLevel)
			}

			setupTables(ddbClient, c.table)
			defer cleanupTables(ddbClient, c.table)

			for k, v := range c.dsEntries {
				err := c.ddbDS.Put(ctx, ds.NewKey(k), []byte(v))
				if c.expPutError != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), c.expPutError)
					return
				}
				require.NoError(t, err)
			}
			for i, q := range c.queries {
				// we run this in a func so we can defer closing the result stream
				func() {
					log.Debugw("test querying", "Query", q)
					res, err := c.ddbDS.Query(ctx, q)
					if res != nil {
						defer res.Close()
					}
					if c.expQueryErrors != nil && c.expQueryErrors[i] != "" {
						require.Error(t, err)
						require.Contains(t, err.Error(), c.expQueryErrors[i])
						return
					}
					require.NoError(t, err)

					// collect the results
					// we don't do this with Rest() since it short-circuits on errors
					var results []query.Result
					for {
						result, ok := res.NextSync()
						if !ok {
							log.Debugw("not ok result", "Result", result)
							break
						}
						results = append(results, result)
						log.Debugw("test got query result", "Result", result)
					}

					// assert the results
					assert.Equal(t, len(c.expResults[i]), len(results))
					for resultIdx, exp := range c.expResults[i] {
						// TODO: compare the whole entry
						result := results[resultIdx]
						if exp.err != "" {
							require.Error(t, result.Error)
							require.Contains(t, result.Error.Error(), exp.err)
							continue
						}
						require.NoError(t, result.Error)

						assert.Equal(t, exp.entry.Key, result.Key)
						assert.Equal(t, exp.entry.Value, result.Value)
					}
				}()
			}
		})
	}
}
