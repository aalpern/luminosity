package luminosity

import (
	"path/filepath"
	"strings"
)

type CatalogPreviews struct {
	Catalog *Catalog

	db *DB
}

// OpenCatalogPreviews
func openCatalogPreviews(cat *Catalog) (*CatalogPreviews, error) {
	p := &CatalogPreviews{Catalog: cat}

	if db, err := OpenDB(p.PreviewsDbPath()); err != nil {
		return nil, err
	} else {
		p.db = db
		return p, nil
	}
}

func (c *CatalogPreviews) Close() error {
	if err := c.db.Close(); err != nil {
		return err
	}
	return nil
}

func (c *CatalogPreviews) PreviewsRootPath() string {
	dir, cat := filepath.Split(c.Catalog.Path())
	basename := strings.TrimSuffix(cat, CatalogExtension)
	return filepath.Join(dir, basename+" Previews.lrdata")
}

func (c *CatalogPreviews) PreviewsDbPath() string {
	return filepath.Join(c.PreviewsRootPath(), "previews.db")
}
