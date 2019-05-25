package luminosity

import (
	"bytes"
	"database/sql"

	"gopkg.in/guregu/null.v3"
)

type CollectionType int

const (
	CollectionTypeStandard CollectionType = iota
	CollectionTypeSmart
)

func (c CollectionType) String() string {
	switch c {
	case CollectionTypeStandard:
		return "standard"
	case CollectionTypeSmart:
		return "smart"
	default:
		return "unknown"
	}
}

func (c CollectionType) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBufferString(`"`)
	buf.WriteString(c.String())
	buf.WriteString(`"`)
	return buf.Bytes(), nil
}

type Collection struct {
	Id       string         `json:"id"`
	Name     string         `json:"name"`
	ParentId null.String    `json:"parent_id"`
	Type     CollectionType `json:"type"`
	Parent   *Collection    `json:"-"`
	Children []*Collection  `json:"children,omitempty"`
}

func (c *Collection) scan(row *sql.Rows) error {
	var collectionType string
	if err := row.Scan(&c.Id, &c.Name, &c.ParentId, &collectionType); err != nil {
		return err
	}
	switch collectionType {
	case "com.adobe.ag.library.smart_collection":
		c.Type = CollectionTypeSmart
	case "com.adobe.ag.library.collection":
		fallthrough
	default:
		c.Type = CollectionTypeStandard
	}
	return nil
}

func (c *Catalog) GetCollections() ([]*Collection, error) {
	const query = `
SELECT  id_local as id,
	    name, 
        parent,
	    creationId
FROM    AgLibraryCollection c
WHERE  c.systemOnly  = 0
AND    c.creationId != 'com.adobe.ag.library.group'
`
	if c.Collections != nil {
		return c.Collections, nil
	}
	if rows, err := c.query("get_collections", query); err != nil {
		return nil, err
	} else {
		var collections []*Collection
		for rows.Next() {
			c := &Collection{}
			if err := c.scan(rows); err != nil {
				return collections, err
			}
			collections = append(collections, c)
		}
		c.Collections = collections
		return c.Collections, nil
	}
}

func (c *Catalog) GetCollectionTree() (*Collection, error) {
	return &Collection{}, nil
}
