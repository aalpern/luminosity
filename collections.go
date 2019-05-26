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
	CollectionTypeGroup
)

func (c CollectionType) String() string {
	switch c {
	case CollectionTypeStandard:
		return "standard"
	case CollectionTypeSmart:
		return "smart"
	case CollectionTypeGroup:
		return "group"
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
	Name     null.String    `json:"name"`
	ParentId null.String    `json:"parent_id,omitempty"`
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
	case "com.adobe.ag.library.group":
		c.Type = CollectionTypeGroup
	case "com.adobe.ag.library.collection":
		fallthrough
	default:
		c.Type = CollectionTypeStandard
	}
	return nil
}

// GetCollections returns a flat list of the collections defined in
// the catalog, excluding collection nodes which are purely structural
// and do not contain photos (i.e. collection groups). System
// collections, such as the always present "Quick Collection", are
// also ignored.
func (c *Catalog) GetCollections() ([]*Collection, error) {
	const query = `
SELECT   id_local,
	     name, 
         parent,
	     creationId
FROM     AgLibraryCollection
WHERE    systemOnly  = 0
AND      creationId != 'com.adobe.ag.library.group'
ORDER BY creationId, name, parent
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

// GetCollectionTree returns all collections in the catalog while
// maintaining the hierarchical relationship of grouped
// collections. Because there can be multiple collection tree roots in
// the Lightroom catalog, they are returned under a dummy root node.
func (c *Catalog) GetCollectionTree() (*Collection, error) {
	const query = `
SELECT   id_local,
	     name, 
         parent,
	     creationId
FROM     AgLibraryCollection c
WHERE    c.systemOnly  = 0
ORDER BY parent, name
`
	if c.CollectionTree != nil {
		return c.CollectionTree, nil
	}
	if rows, err := c.query("get_collection_tree", query); err != nil {
		return nil, err
	} else {
		root := &Collection{
			Name:     null.StringFrom("Root"),
			Children: []*Collection{},
			Type:     CollectionTypeGroup,
		}
		collections := map[string]*Collection{}

		defer rows.Close()
		for rows.Next() {
			c := &Collection{}
			if err := c.scan(rows); err != nil {
				return nil, err
			}
			collections[c.Id] = c
		}

		for _, c := range collections {
			parent := root
			parentid := c.ParentId.ValueOrZero()
			if parentid != "" {
				parent = collections[parentid]
			}
			c.Parent = parent
			parent.Children = append(parent.Children, c)
		}

		c.CollectionTree = root
		return c.CollectionTree, nil
	}
}
