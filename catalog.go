// A library for accessing Adobe Lightroom catalogs.
package luminosity

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// Catalog represents a Lightroom catalog and all the information
// extracted from it.
type Catalog struct {
	db *sql.DB

	// Paths is a list of filepaths to .lrcat files. There will only
	// be more than one value in Paths if multiple catalog objects
	// have been merged together for aggregate stats via the Merge()
	// function.
	Paths   []string        `json:"paths"`
	Lenses  NamedObjectList `json:"lenses"`
	Cameras NamedObjectList `json:"cameras"`
	Stats   *Stats          `json:"stats"`
	Photos  []*PhotoRecord  `json:"photos"`
}

// NewCatalog allocates and initializes a new Catalog instance without
// a database connection, for merging other loaded catalogs into.
func NewCatalog() *Catalog {
	return &Catalog{
		Stats:   newStats(),
		Lenses:  NamedObjectList{},
		Cameras: NamedObjectList{},
	}
}

// OpenCatalog initializes a new Catalog struct and opens a connection
// to the database file, but does not load any data. OpenCatalog will
// fail if the catalog is currently open in Lightroom.
func OpenCatalog(path string) (*Catalog, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &Catalog{
		db: db,
		Paths: []string{
			path,
		},
		Stats:   newStats(),
		Lenses:  NamedObjectList{},
		Cameras: NamedObjectList{},
	}, nil
}

// Close closes the underlying database file.
func (c *Catalog) Close() error {
	return c.db.Close()
}

// Load retrieves everything luminosity knows about the lightroom
// catalog - lenses, cameras, statistics, and summary metadata for
// every photo.
func (c *Catalog) Load() error {
	lenses, err := c.GetLenses()
	if err != nil {
		return err
	}
	c.Lenses = lenses

	cameras, err := c.GetCameras()
	if err != nil {
		return err
	}
	c.Cameras = cameras

	stats, err := c.GetStats()
	if err != nil {
		return err
	}
	c.Stats = stats

	photos, err := c.GetPhotos()
	if err != nil {
		return err
	}
	c.Photos = photos

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
		c.Stats.Merge(other.Stats)
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
}

// GetLenses returns a list of every lens name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetLenses() (NamedObjectList, error) {
	return c.queryNamedObjects("select id_local, value from AgInternedExifLens")
}

// GetCameras returns a list of every camera name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetCameras() (NamedObjectList, error) {
	return c.queryNamedObjects("select id_local, value from AgInternedExifCameraModel")
}

func (c *Catalog) queryNamedObjects(sql string) (NamedObjectList, error) {
	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return convertNamedObjects(rows)
}
