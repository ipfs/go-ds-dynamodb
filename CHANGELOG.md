# Changelog

All notable changes to this project will be documented in this file.

Note:
* The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
* This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Legend
The following emojis are used to highlight certain changes:
* 🛠 - BREAKING CHANGE. Action is required if you use this functionality.
* ✨ - Noteworthy change to be aware of.

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

### Security

## [v0.3.0] - 2026-05-27

### Changed

- 🛠 Migrated from `aws-sdk-go` (v1, end-of-support as of v1.55.8) to `aws-sdk-go-v2` ([#22](https://github.com/ipfs/go-ds-dynamodb/pull/22)). `New()` now accepts `*dynamodb.Client` from `github.com/aws/aws-sdk-go-v2/service/dynamodb` instead of `*dynamodb.DynamoDB` from `github.com/aws/aws-sdk-go`. Attribute marshaling moves to `feature/dynamodb/attributevalue`. `AttributeValue` becomes an interface with `types.AttributeValueMember*` concrete members. `ExpressionAttributeNames` is now `map[string]string`, since v2 dropped `*string`. Errors are matched with `errors.As` against `*types.ConditionalCheckFailedException` and `*types.ResourceInUseException` instead of `awserr.Error.Code()`. Queries page through `dynamodb.NewQueryPaginator`. Every `*WithContext` method collapses to its ctx-first equivalent.

    **Action required.** Build clients with the v2 SDK and pass them to `ddbds.New`:

    ```go
    // before
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    ddbDS := ddbds.New(dynamodb.New(sess), "datastore-table")

    // after
    cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
    if err != nil {
        return err
    }
    ddbDS := ddbds.New(dynamodb.NewFromConfig(cfg), "datastore-table")
    ```

- Bumped non-AWS direct dependencies: `go-datastore` v0.8.2 to v0.9.1, `go-log/v2` v2.5.1 to v2.9.2, `testify` v1.10.0 to v1.11.1.

### Removed

### Fixed

- Scan worker no longer panics on `Scan` errors. v1 returned a zero-value response on error, so the post-error iteration over `res.Items` was harmless; v2 returns nil and the iteration crashed. The worker now sends the error and returns. [`d2b4528`](https://github.com/ipfs/go-ds-dynamodb/commit/d2b4528)
- Batch commit no longer races on a shared `*rand.Rand`. Jitter uses `math/rand/v2`, whose top-level functions are safe for concurrent use. [`f93f955`](https://github.com/ipfs/go-ds-dynamodb/commit/f93f955)
- `Put` on a sort-key table returns `ErrInvalidKey` for keys with too few namespaces, instead of panicking in `putKey` with "assignment to entry in nil map". [`669f8e7`](https://github.com/ipfs/go-ds-dynamodb/commit/669f8e7)
- Batch retry backoff stops when the `Commit` context is cancelled. Used to sleep through the full backoff. [`d1a78a5`](https://github.com/ipfs/go-ds-dynamodb/commit/d1a78a5)
- Batch retry exhaustion error reads "batch had unprocessed items after N attempts" instead of the previous `last error: %!w(<nil>)` produced by wrapping a nil error with `%w`. When a real error is present it is still wrapped as `last error: <err>`. [`9f93267`](https://github.com/ipfs/go-ds-dynamodb/commit/9f93267) [`52addd0`](https://github.com/ipfs/go-ds-dynamodb/commit/52addd0)
- `DiskUsage` and `EntryCount` nil-check `res.Table` and use `aws.ToInt64` for the `*int64` fields, instead of dereferencing them blindly. [`3538832`](https://github.com/ipfs/go-ds-dynamodb/commit/3538832)
- `Query` no longer relies on an unchecked type assertion on the partition-key `AttributeValue`. `queryKey` now returns the value directly. [`95e0498`](https://github.com/ipfs/go-ds-dynamodb/commit/95e0498)
- Batch `Commit` no longer deadlocks when cancelled mid-retry. The internal error channel is buffered to the chunk count, so the chunk goroutine's deferred send always completes without racing against `<-ctx.Done()`. [`b7486ea`](https://github.com/ipfs/go-ds-dynamodb/commit/b7486ea)
- `SetTTL` works on tables whose partition key has been renamed via `WithPartitionkey`. The `UpdateItem` request used to include a stray `DSKey` attribute and DynamoDB rejected every call with `ValidationException`. [`c1a28ca`](https://github.com/ipfs/go-ds-dynamodb/commit/c1a28ca)

### Security
