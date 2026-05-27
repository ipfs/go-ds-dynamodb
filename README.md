go-ds-dynamodb
=======================

> A DynamoDB Datastore Implementation

This is an implementation of [go-datastore](https://github.com/ipfs/go-datastore) that is backed by DynamoDB. It uses [`aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2).

ddbds supports optimized prefix queries. When the table's key schema matches an incoming query, ddbds issues a DynamoDB `Query` instead of a table scan, enabling ordered, high-cardinality prefix queries.

> [!WARNING]
> ddbds stores values up to 400 kB, the DynamoDB maximum item size, so it is not suitable for block storage. Within IPFS, use it for DHT records, IPNS records, peerstore records, and similar small-value workloads.

## Setup ##

### Simple Setup with Unoptimized Queries ###
Use ddbds as a plain key-value store when optimized queries are not needed.

Datastore queries then run as parallel table scans, and filtering, ordering, and limits are applied client-side. This is a good fit for small tables or workloads that do not benefit from server-side ordering.

```go
import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ipfs/go-ds-dynamodb"
)

cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
if err != nil {
	return err
}
ddbDS := ddbds.New(dynamodb.NewFromConfig(cfg), "datastore-table")
```

The default partition key is `DSKey` of type `string`. Override it with `WithPartitionkey()`.

### Optimized Queries ###
Optimized prefix queries require a sort key, and every key written must have at least two parts (for example `/a/b`, not `/a`).

`ddbds` splits the datastore key into partition and sort key components:

* `/a` -> error (not enough parts)
* `/a/b` -> [`a`, `b`]
* `/a/b/c` -> [`a`, `b/c`]
* etc.

Enable optimized queries by setting the sort key name with `WithSortKey()`:

```go
ddbDS := ddbds.New(
	dynamodb.NewFromConfig(cfg),
	"datastore-table",
	ddbds.WithPartitionkey("PartitionKey"),
	ddbds.WithSortKey("SortKey"),
)
```

### Other Options ###

* `WithStronglyConsistentReads()` issues strongly consistent reads on `Get`, `Has`, and `Query`. Reads cost twice as much and have higher latency, but reflect the latest writes.
* `WithScanParallelism(n)` sets the segment count for parallel `Scan` requests used by unoptimized queries. Defaults to `1`.

### Composing Datastores ###
Compose ddbds with a mount datastore to dispatch each namespace to a table tuned for its access pattern.

```go
ddbClient := dynamodb.NewFromConfig(cfg)
ddbDS := mount.New([]mount.Mount{
	{
		Prefix: ds.NewKey("/peers/addrs"),
		Datastore: ddbds.New(
			ddbClient,
			"datastore-peers-addrs",
			ddbds.WithPartitionkey("PeerID"),
		),
	},
	{
		Prefix: ds.NewKey("/providers"),
		Datastore: ddbds.New(
			ddbClient,
			"datastore-providers",
			ddbds.WithPartitionkey("ContentHash"),
			ddbds.WithSortKey("PeerID"),
		),
	},
	{
		Prefix: ds.NewKey("/"),
		Datastore: ddbds.New(
			ddbClient,
			"datastore-all",
		),
	},
})
```

### IAM Permissions ###
Each datastore method maps to one DynamoDB API action:

* `dynamodb:GetItem` - `Get()`, `GetExpiration()`, `GetSize()`, `Has()`
* `dynamodb:PutItem` - `Put()`, `PutWithTTL()`
* `dynamodb:DeleteItem` - `Delete()`
* `dynamodb:Scan` - `Query()` when no sort key is configured
* `dynamodb:Query` - `Query()` when a sort key is configured
* `dynamodb:DescribeTable` - `DiskUsage()`, `EntryCount()`
* `dynamodb:UpdateItem` - `SetTTL()`
* `dynamodb:BatchWriteItem` - `Batch.Commit()`

## Datastore Features ##

* [x] Batching
* [x] TTL
* [x] Disk Usage
* [ ] Transactions
* [ ] Checked (not applicable)
* [ ] Scrubbed (not applicable)
* [ ] GC (not applicable)

## Contributing

Contributions are welcome! This repository is part of the IPFS project and therefore governed by our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
