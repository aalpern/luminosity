// A library for accessing Adobe Lightroom catalogs.
package luminosity

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

// Catalog represents a Lightroom catalog and all the information
// extracted from it.
type Catalog struct {
	db *sql.DB

	// paths is a list of filepaths to .lrcat files. There will only
	// be more than one value in Paths if multiple catalog objects
	// have been merged together for aggregate stats via the Merge()
	// function.
	paths   []string
	lenses  NamedObjectList
	cameras NamedObjectList
	stats   *Stats
	photos  []*PhotoRecord
}

func (c *Catalog) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"paths":   c.paths,
		"lenses":  c.lenses,
		"cameras": c.cameras,
		"photos":  c.photos,
		"stats":   c.stats,
	}
	return json.Marshal(m)
}

// NewCatalog allocates and initializes a new Catalog instance without
// a database connection, for merging other loaded catalogs into.
func NewCatalog() *Catalog {
	return &Catalog{}
}

// OpenCatalog initializes a new Catalog struct and opens a connection
// to the database file, but does not load any data. OpenCatalog will
// fail if the catalog is currently open in Lightroom.
func OpenCatalog(path string) (*Catalog, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"action": "catalog_open",
		"path":   path,
		"status": "ok",
	}).Debug()
	return &Catalog{
		db: db,
		paths: []string{
			path,
		},
	}, nil
}

// Close closes the underlying database file.
func (c *Catalog) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Load retrieves everything luminosity knows about the lightroom
// catalog - lenses, cameras, statistics, and summary metadata for
// every photo.
func (c *Catalog) Load() error {
	if _, err := c.GetLenses(); err != nil {
		return err
	}
	if _, err := c.GetCameras(); err != nil {
		return err
	}
	if _, err := c.GetStats(); err != nil {
		return err
	}
	if _, err := c.GetPhotos(); err != nil {
		return err
	}
	return nil
}

// Merge takes the loaded contents of another catalog and merges them
// into the target. Named objects are kept unique according to their
// names.
func (c *Catalog) Merge(other *Catalog) {
	if other == nil {
		return
	}
	if other.paths != nil {
		c.paths = append(c.paths, other.paths...)
	}
	if other.stats != nil {
		stats, _ := c.GetStats()
		stats.Merge(other.stats)
	}
	if other.cameras != nil {
		c.cameras = c.cameras.Merge(other.cameras)
	}
	if other.lenses != nil {
		c.lenses = c.lenses.Merge(other.lenses)
	}
	if other.photos != nil {
		c.photos = append(c.photos, other.photos...)
	}
}

// GetLenses returns a list of every lens name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetLenses() (NamedObjectList, error) {
	if c.lenses != nil {
		return c.lenses, nil
	}
	lenses, err := c.queryNamedObjects("select id_local, value from AgInternedExifLens")
	if err != nil {
		return nil, err
	}
	c.lenses = lenses
	return c.lenses, nil
}

// GetCameras returns a list of every camera name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetCameras() (NamedObjectList, error) {
	if c.cameras != nil {
		return c.cameras, nil
	}
	cameras, err := c.queryNamedObjects("select id_local, value from AgInternedExifCameraModel")
	if err != nil {
		return nil, err
	}
	c.cameras = cameras
	return c.cameras, nil
}
