// A library for accessing Adobe Lightroom catalogs.
package luminosity

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

type catalog struct {
	Paths       []string        `json:"paths"`
	Lenses      NamedObjectList `json:"lenses"`
	Cameras     NamedObjectList `json:"cameras"`
	Stats       *Stats          `json:"stats"`
	Collections []*Collection   `json:"collections"`
	Photos      []*PhotoRecord  `json:"-"`
}

// Catalog represents a Lightroom catalog and all the information
// extracted from it.
type Catalog struct {
	catalog
	db *sql.DB
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
	cat := &Catalog{
		db: db,
	}
	cat.Paths = []string{
		path,
	}
	return cat, nil
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
	if _, err := c.GetCollections(); err != nil {
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
	if other.Paths != nil {
		c.Paths = append(c.Paths, other.Paths...)
	}
	if other.Stats != nil {
		stats, _ := c.GetStats()
		stats.Merge(other.Stats)
	}
	if other.Cameras != nil {
		c.Cameras = c.Cameras.Merge(other.Cameras)
	}
	if other.Lenses != nil {
		c.Lenses = c.Lenses.Merge(other.Lenses)
	}
	if other.Photos != nil {
		c.Photos = append(c.Photos, other.Photos...)
	}
	if other.Collections != nil {
		c.Collections = append(c.Collections, other.Collections...)
	}
}

// GetLenses returns a list of every lens name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetLenses() (NamedObjectList, error) {
	if c.Lenses != nil {
		return c.Lenses, nil
	}
	lenses, err := c.queryNamedObjects("select id_local, value from AgInternedExifLens")
	if err != nil {
		return nil, err
	}
	c.Lenses = lenses
	return c.Lenses, nil
}

// GetCameras returns a list of every camera name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetCameras() (NamedObjectList, error) {
	if c.Cameras != nil {
		return c.Cameras, nil
	}
	cameras, err := c.queryNamedObjects("select id_local, value from AgInternedExifCameraModel")
	if err != nil {
		return nil, err
	}
	c.Cameras = cameras
	return c.Cameras, nil
}
