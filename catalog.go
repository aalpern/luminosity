// A library for accessing Adobe Lightroom catalogs.
package luminosity

import (
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	CatalogExtension        = ".lrcat"
	CatalogDataDirExtension = ".lrdata"
)

type catalog struct {
	Paths          []string        `json:"paths"`
	Lenses         NamedObjectList `json:"lenses"`
	Cameras        NamedObjectList `json:"cameras"`
	Stats          *Stats          `json:"stats"`
	Collections    []*Collection   `json:"collections"`
	CollectionTree *Collection     `json:"collection_tree"`
	Photos         []*PhotoRecord  `json:"-"`
}

// Catalog represents a Lightroom catalog and all the information
// extracted from it.
type Catalog struct {
	catalog

	// Connection to the primary catalog database file.
	db *DB

	// Preview store for the cached Lightroom previews, if
	// present. This is initialized lazily.
	previews *CatalogPreviews
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
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}

	db, err := OpenDB(path)
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

func (c *Catalog) Path() string {
	return c.Paths[0]
}

func (c *Catalog) Name() string {
	return strings.TrimSuffix(filepath.Base(c.Path()), CatalogExtension)
}

func (c *Catalog) Previews() (*CatalogPreviews, error) {
	if c.previews != nil {
		return c.previews, nil
	}
	if p, err := openCatalogPreviews(c); err != nil {
		return nil, err
	} else {
		c.previews = p
		return c.previews, nil
	}
}

// Close closes the underlying database file(s).
func (c *Catalog) Close() error {
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return err
		}
	}
	if c.previews != nil {
		if err := c.previews.Close(); err != nil {
			return err
		}
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
	if _, err := c.GetCollectionTree(); err != nil {
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
	if other.CollectionTree != nil && c.CollectionTree == nil {
		c.CollectionTree = other.CollectionTree
	}
}

// GetLenses returns a list of every lens name extracted from EXIF
// metadata by Lightroom.
func (c *Catalog) GetLenses() (NamedObjectList, error) {
	if c.Lenses != nil {
		return c.Lenses, nil
	}
	lenses, err := c.db.queryNamedObjects("select id_local, value from AgInternedExifLens")
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
	cameras, err := c.db.queryNamedObjects("select id_local, value from AgInternedExifCameraModel")
	if err != nil {
		return nil, err
	}
	c.Cameras = cameras
	return c.Cameras, nil
}

// FindCatalogs returns the full pathnames of every Lightroom catalog
// (.lrcat) file in the list of inputs paths. Any directories in paths
// will be walked recursively.
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
