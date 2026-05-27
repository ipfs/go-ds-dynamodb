package ddbds

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

func (d *DDBDatastore) queryKey(queryPrefix ds.Key) (map[string]types.AttributeValue, bool) {
	queryPrefixNamespaces := namespaces(queryPrefix)

	if len(queryPrefixNamespaces) == 0 {
		return nil, false
	}

	return map[string]types.AttributeValue{
		d.partitionKey: &types.AttributeValueMemberS{Value: queryPrefixNamespaces[0]},
	}, true
}

func (d *DDBDatastore) putKey(key ds.Key) (map[string]types.AttributeValue, bool) {
	attrs, ok := d.getKey(key)
	if !ok {
		return nil, false
	}
	attrs[attrNameKey] = &types.AttributeValueMemberS{Value: key.String()}
	return attrs, true
}

func (d *DDBDatastore) getKey(key ds.Key) (map[string]types.AttributeValue, bool) {
	keyNamespaces := namespaces(key)

	attrs := map[string]types.AttributeValue{}

	if d.sortKey == "" {
		attrs[d.partitionKey] = &types.AttributeValueMemberS{Value: key.String()}
	} else {
		// if there's a sort key, then the first element of the trimmed key is the partition key
		// and the rest of the trimmed key is the sort key

		// there need to be >= 2 elements in the trimmed key so we can derive a sort key
		// otherwise we can't write to this table
		if len(keyNamespaces) < 2 {
			return nil, false
		}

		attrs[d.partitionKey] = &types.AttributeValueMemberS{Value: keyNamespaces[0]}
		attrs[d.sortKey] = &types.AttributeValueMemberS{Value: strings.Join(keyNamespaces[1:], "/")}
	}

	return attrs, true
}
