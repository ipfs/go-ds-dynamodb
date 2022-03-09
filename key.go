package ddbds

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
)

func namespaces(k ds.Key) []string {
	namespaces := k.Namespaces()

	// for the root key "/", we want an empty/nil slice, not [""]
	if len(namespaces) == 1 && namespaces[0] == "" {
		namespaces = nil
	}
	return namespaces
}

func (d *DDBDatastore) queryKey(queryPrefix string) (map[string]*dynamodb.AttributeValue, bool) {
	queryPrefixKey := ds.NewKey(queryPrefix)
	queryPrefixNamespaces := namespaces(queryPrefixKey)

	if len(queryPrefixNamespaces) == 0 {
		return nil, false
	}

	return map[string]*dynamodb.AttributeValue{
		d.partitionKey: {S: &queryPrefixNamespaces[0]},
	}, true
}

func (d *DDBDatastore) putKey(key ds.Key) (map[string]*dynamodb.AttributeValue, bool) {
	attrs, ok := d.getKey(key)
	attrs[attrNameKey] = &dynamodb.AttributeValue{S: aws.String(key.String())}
	return attrs, ok
}

func (d *DDBDatastore) getKey(key ds.Key) (map[string]*dynamodb.AttributeValue, bool) {
	keyNamespaces := namespaces(key)

	attrs := map[string]*dynamodb.AttributeValue{}

	if d.sortKey == "" {
		partitionKey := key.String()
		attrs[d.partitionKey] = &dynamodb.AttributeValue{S: &partitionKey}
	} else {
		// if there's a sort key, then the first element of the trimmed key is the partition key
		// and the rest of the trimmed key is the sort key

		// there need to be >= 2 elements in the trimmed key so we can derive a sort key
		// otherwise we can't write to this table
		if len(keyNamespaces) < 2 {
			return nil, false
		}

		partitionKey := keyNamespaces[0]
		attrs[d.partitionKey] = &dynamodb.AttributeValue{S: &partitionKey}

		sortKey := strings.Join(keyNamespaces[1:], "/")
		attrs[d.sortKey] = &dynamodb.AttributeValue{S: &sortKey}
	}

	return attrs, true
}
