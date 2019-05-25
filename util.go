package luminosity

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/guregu/null.v3"
)

type NamedObject struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type NamedObjectList []*NamedObject
type NamedObjectMap map[string]*NamedObject

type ByName NamedObjectList

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

func (l NamedObjectList) ToMap() NamedObjectMap {
	m := NamedObjectMap{}
	for _, o := range l {
		m[o.Name] = o
	}
	return m
}

func (m NamedObjectMap) ToList() NamedObjectList {
	l := NamedObjectList{}
	for _, o := range m {
		l = append(l, o)
	}
	sort.Sort(ByName(l))
	return l
}

func (l NamedObjectList) Merge(other NamedObjectList) NamedObjectList {
	m := l.ToMap()
	for _, o := range other {
		if _, ok := m[o.Name]; !ok {
			m[o.Name] = o
		}
	}
	l2 := m.ToList()
	sort.Sort(ByName(l2))
	return l2
}

func (c *Catalog) query(label, sql string) (*sql.Rows, error) {
	fields := log.Fields{
		"action": "query",
		"status": "ok",
		"label":  label,
		"sql":    sql,
	}
	rows, err := c.db.Query(sql)
	if err != nil {
		fields["status"] = "error"
		fields["error"] = err
	}
	log.WithFields(fields).Debug("Executed query")
	return rows, err
}

func (c *Catalog) queryStringMap(label, sql string) ([]map[string]string, error) {
	var results []map[string]string
	if rows, err := c.query(label, sql); err != nil {
		return results, err
	} else {
		for rows.Next() {
			columns, err := rows.Columns()
			if err != nil {
				return results, err
			}

			colcount := len(columns)
			values := make([]null.String, colcount, colcount)
			valueptrs := make([]interface{}, colcount, colcount)
			for i, _ := range values {
				valueptrs[i] = &values[i]
			}
			if err := rows.Scan(valueptrs...); err != nil {
				return results, err
			}

			m := map[string]string{}
			for i, col := range columns {
				m[col] = values[i].ValueOrZero()
			}
			results = append(results, m)
		}
	}
	return results, nil
}

func (c *Catalog) queryNamedObjects(sql string) (NamedObjectList, error) {
	rows, err := c.query("query_named_objects", sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return convertNamedObjects(rows)
}

func convertNamedObjects(rows *sql.Rows) (NamedObjectList, error) {
	var objects NamedObjectList
	for rows.Next() {
		var name null.String
		obj := &NamedObject{}
		if err := rows.Scan(&obj.Id, &name); err != nil {
			return nil, err
		}
		obj.Name = name.String
		objects = append(objects, obj)
	}
	log.WithFields(log.Fields{
		"action": "convert_named_objects",
		"count":  len(objects),
	}).Debug()
	return objects, nil
}

// ----------------------------------------------------------------------

const (
	CatalogExtension        = "lrcat"
	CatalogDataDirExtension = "lrdata"
)

func FindCatalogs(paths ...string) []string {
	found := make([]string, 0, len(paths))

	// For each path in the input
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			log.WithFields(log.Fields{
				"action": "find_catalogs",
				"status": "stat_error",
				"path":   path,
				"error":  "err",
			}).Warn("Cannot stat path")
			continue
		}

		// Process files
		if !info.IsDir() {
			if strings.HasSuffix(path, CatalogExtension) {
				found = append(found, path)
			}
		} else {
			// Process directories
			children := findCatalogsInDir(path)
			found = append(found, children...)
		}
	}
	return found
}

func findCatalogsInDir(path string) []string {
	found := make([]string, 0, 8)

	filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"action": "find_catalogs",
				"status": "walk_error",
				"path":   path,
				"error":  "err",
			}).Warn("Error walking path")
		} else if !info.IsDir() {
			found = append(found, FindCatalogs(p)...)
		} else if info.IsDir() {
			// Skip the .lrdata directories which contain the
			// potentially huge number of cached image previews
			if strings.HasSuffix(p, CatalogDataDirExtension) {
				return filepath.SkipDir
			}
		}
		return nil
	})

	return found
}
