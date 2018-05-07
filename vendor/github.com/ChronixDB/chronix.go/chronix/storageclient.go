package chronix

import "time"

// A StorageClient allows updating documents in Solr.
type StorageClient interface {
	Update(data []map[string]interface{}, commit bool, commitWithin time.Duration) error
	// TODO: Return a more interpreted result on the Solr level.
	Query(q, fq, fl string) ([]byte, error)

	NeedPostfixOnDynamicField() bool
}
