package luminosity

import (
	"fmt"
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

func (c *CatalogPreviews) GetPhotoCacheInfo(p *PhotoRecord) (*PhotoCacheInfo, error) {
	const query = `
SELECT ice.imageId, 
       ice.uuid, 
       ice.digest, 
       max(pl.level) 
FROM   ImageCacheEntry ice 
       INNER JOIN PyramidLevel pl 
               ON pl.uuid = ice.uuid 
`
	var where = fmt.Sprintf(" WHERE ice.imageId = %d", p.Id)
	row := c.db.queryRow("get_photo_cache_info", query+where)
	ci := &PhotoCacheInfo{
		previews: c,
	}
	if err := row.Scan(&ci.Id, &ci.UUID, &ci.Digest, &ci.MaxLevel); err != nil {
		return nil, err
	} else {
		return ci, nil
	}
}

type PhotoCacheInfo struct {
	previews *CatalogPreviews
	Id       int    `json:"id"`
	UUID     string `json:"uuid"`
	Digest   string `json:"digest"`
	MaxLevel int    `json:"max_level"`
}

func (ci *PhotoCacheInfo) Path() string {
	return filepath.Join(ci.previews.Path(),
		string(ci.UUID[0]),
		string(ci.UUID[0:4]),
		fmt.Sprintf("%s-%s.lrprev", ci.UUID, ci.Digest))
}
