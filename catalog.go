// A library for accessing Adobe Lightroom catalogs.
package luminosity

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/guregu/null.v3"
)

// Catalog represents a Lightroom catalog and all the information
// extracted from it.
type Catalog struct {
	db      *sql.DB
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

// ----------------------------------------------------------------------
// Photos
// ----------------------------------------------------------------------

const (
	kPhotoRecordSelect = `
SELECT    image.id_local as id,
          rootFolder.absolutePath || folder.pathFromRoot || rootfile.baseName || '.' || rootfile.extension AS fullName,
          coalesce(Lens.value, 'Unknown') as Lens,
          coalesce(Camera.Value, 'Unknown') as Camera,
          image.fileFormat,
          image.fileHeight,
          image.fileWidth,
          image.orientation,
          image.captureTime,
          image.rating,
          image.colorLabels,
          image.pick,
          exif.dateDay,
          exif.dateMonth,
          exif.dateYear,
          exif.flashFired,
          exif.isoSpeedRating,
          exif.shutterSpeed,
          exif.focalLength,
          exif.aperture,
          exif.hasGPS ,
          exif.gpsLatitude,
          exif.gpsLongitude,
          iptc.caption,
          iptc.copyright
`
	kPhotoRecordFrom = `
FROM      Adobe_images              image
JOIN      AgLibraryFile             rootFile   ON   rootfile.id_local =    image.rootFile
JOIN      AgLibraryFolder           folder     ON     folder.id_local = rootfile.folder
JOIN      AgLibraryRootFolder       rootFolder ON rootFolder.id_local =   folder.rootFolder
LEFT JOIN AgLibraryIPTC             iptc       ON      image.id_local =     iptc.image
LEFT JOIN AgharvestedExifMetadata   exif       ON      image.id_local =     exif.image
LEFT JOIN AgInternedExifLens        Lens       ON       Lens.id_Local =     exif.lensRef
LEFT JOIN AgInternedExifCameraModel Camera     ON     Camera.id_local =     exif.cameraModelRef
`
	kPhotoRecordListOrderBy = "ORDER BY FullName"
)

// PhotoRecord gathers the most commonly used information about each
// photo into a single record, extracted from 8 different tables in
// the Lightroom catalog.
type PhotoRecord struct {
	Id       string      `json:"id"`
	FullName string      `json:"full_name"`
	Lens     null.String `json:"lens"`
	Camera   null.String `json:"camera"`

	// Image table
	FileFormat  string      `json:"file_format"`
	FileHeight  null.Int    `json:"file_height"`
	FileWidth   null.Int    `json:"file_width"`
	Orientation null.String `json:"orientation"`
	CaptureTime time.Time   `json:"capture_time"`
	Rating      null.String `json:"rating"`
	ColorLabels string      `json:"color_labels"`
	Pick        null.Int    `json:"pick"`

	// Exif
	DateDay      null.Int    `json:"date_day"`
	DateMonth    null.Int    `json:"date_month"`
	DateYear     null.Int    `json:"date_year"`
	FlashFired   null.Bool   `json:"flash_fired"`
	ISO          null.String `json:"iso"`
	ShutterSpeed float64     `json:"shutter_speed"`
	ExposureTime string      `json:"exposure_time"`
	FocalLength  null.String `json:"focal_length"`
	Aperture     float64     `json:"aperture"`
	FNumber      string      `json:"fnumber"`
	HasGPS       bool        `json:"has_gps"`
	Latitude     null.Float  `json:"lat"`
	Longitude    null.Float  `json:"lon"`

	// Iptc
	Caption   null.String `json:"caption"`
	Copyright null.String `json:"copyright"`
}

func parseTime(s string) (time.Time, error) {
	var formats = []string{
		"2006-01-02T15:04:05+07:00",
		"2006-01-02T15:04:05",
	}
	var err error
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}

func (p *PhotoRecord) scan(row *sql.Rows) error {
	var capTime null.String
	var apertureString null.String
	var shutterSpeedString null.String
	err := row.Scan(&p.Id, &p.FullName, &p.Lens, &p.Camera,
		// Image
		&p.FileFormat, &p.FileHeight, &p.FileWidth, &p.Orientation, &capTime, &p.Rating, &p.ColorLabels, &p.Pick,
		// Exif
		&p.DateDay, &p.DateMonth, &p.DateYear, &p.FlashFired, &p.ISO, &shutterSpeedString, &p.FocalLength, &apertureString,
		&p.HasGPS, &p.Latitude, &p.Longitude,
		// Iptc
		&p.Caption, &p.Copyright,
	)
	if err != nil {
		return err
	}

	if capTime.Valid {
		p.CaptureTime, err = parseTime(capTime.String)
		if err != nil {
			return err
		}
	}

	if shutterSpeedString.Valid {
		shutterSpeed, err := strconv.ParseFloat(shutterSpeedString.String, 64)
		if err != nil {
			return err
		}
		p.ShutterSpeed = shutterSpeed
		p.ExposureTime = ShutterSpeedToExposureTime(shutterSpeed)
	}

	if apertureString.Valid {
		aperture, err := strconv.ParseFloat(apertureString.String, 64)
		if err != nil {
			return err
		}
		p.Aperture = aperture
		p.FNumber = fmt.Sprintf("%.1f", ApertureToFNumber(aperture))
	}
	return nil
}

// ForEachPhoto takes a handler function and calls it successively on
// a PhotoRecord structure for every photo in the catalog. Returning
// an error from the handler function will stop the iteration.
func (c *Catalog) ForEachPhoto(handler func(*PhotoRecord) error) error {
	rows, err := c.db.Query(kPhotoRecordSelect + kPhotoRecordFrom + kPhotoRecordListOrderBy)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		p := &PhotoRecord{}
		err = p.scan(rows)
		if err != nil {
			return err
		}
		handler(p)
	}
	return nil
}

// GetPhotoCount returns a simple count of the total number of images
// stored in the catalog.
func (c *Catalog) GetPhotoCount() (int64, error) {
	row := c.db.QueryRow("select count(*) " + kPhotoRecordFrom)
	var count int64 = -1
	err := row.Scan(&count)
	return count, err
}

// GetPhotos returns an array of PhotoRecord structs for every photo
// represented in the catalog.
func (c *Catalog) GetPhotos() ([]*PhotoRecord, error) {
	count, err := c.GetPhotoCount()
	if err != nil {
		return nil, err
	}
	photos := make([]*PhotoRecord, 0, count)
	err = c.ForEachPhoto(func(p *PhotoRecord) error {
		photos = append(photos, p)
		return nil
	})
	return photos, err
}

// Not implemented yet
func (c *Catalog) GetPhotoByFilename(fn string) (*PhotoRecord, error) {
	return nil, nil
}

// Not implemented yet
func (c *Catalog) GetPhotoByID(id int64) (*PhotoRecord, error) {
	return nil, nil
}
