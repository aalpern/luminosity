package luminosity

import (
	"path/filepath"
	"strings"
)

type CatalogPreviews struct {
	Catalog *Catalog

	root string
	db   *DB
}

func openCatalogPreviews(cat *Catalog) (*CatalogPreviews, error) {
	p := &CatalogPreviews{
		Catalog: cat,
		root:    previewsRootPath(cat),
	}

	if db, err := OpenDB(p.DbPath()); err != nil {
		return nil, err
	} else {
		p.db = db
		return p, nil
	}
}

func previewsRootPath(cat *Catalog) string {
	dir, file := filepath.Split(cat.Path())
	basename := strings.TrimSuffix(file, CatalogExtension)
	return filepath.Join(dir, basename+" Previews.lrdata")
}

func (c *CatalogPreviews) Close() error {
	if err := c.db.Close(); err != nil {
		return err
	}
	return nil
}

func (c *CatalogPreviews) Path() string {
	return c.root
}

func (c *CatalogPreviews) DbPath() string {
	return filepath.Join(c.root, "previews.db")
}
