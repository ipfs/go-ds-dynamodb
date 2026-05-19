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

- 🛠 Migrated from `aws-sdk-go` (v1, end-of-support as of v1.55.8) to `aws-sdk-go-v2`. `New()` now accepts `*dynamodb.Client` from `github.com/aws/aws-sdk-go-v2/service/dynamodb` instead of `*dynamodb.DynamoDB` from `github.com/aws/aws-sdk-go`. Attribute marshaling moves to `feature/dynamodb/attributevalue`. `AttributeValue` becomes an interface with `types.AttributeValueMember*` concrete members. `ExpressionAttributeNames` is now `map[string]string`, since v2 dropped `*string`. Errors are matched with `errors.As` against `*types.ConditionalCheckFailedException` and `*types.ResourceInUseException` instead of `awserr.Error.Code()`. Queries page through `dynamodb.NewQueryPaginator`. Every `*WithContext` method collapses to its ctx-first equivalent.

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

- Scan worker no longer dereferences a nil `*ScanOutput` when `Scan` returns an error. The v1 SDK masked the bug by returning a non-nil zero-value output on error; v2 returns nil and exposes the panic. The worker now sends the error and returns unconditionally.

### Security
