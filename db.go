package luminosity

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	null "gopkg.in/guregu/null.v3"
)

type DB struct {
	*sql.DB
}

func OpenDB(path string) (*DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) query(label, sql string) (*sql.Rows, error) {
	fields := log.Fields{
		"action": "query",
		"status": "ok",
		"label":  label,
		"sql":    sql,
	}
	rows, err := db.DB.Query(sql)
	if err != nil {
		fields["status"] = "error"
		fields["error"] = err
	}
	log.WithFields(fields).Debug("Executed query")
	return rows, err
}

func (db *DB) queryStringMap(label, sql string) ([]map[string]string, error) {
	var results []map[string]string
	if rows, err := db.query(label, sql); err != nil {
		return results, err
	} else {
		defer rows.Close()
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

func (db *DB) queryNamedObjects(sql string) (NamedObjectList, error) {
	rows, err := db.query("query_named_objects", sql)
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
