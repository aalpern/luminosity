package luminosity

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/guregu/null.v3"
)

// ----------------------------------------------------------------------
// NamedObject
// ----------------------------------------------------------------------

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
	return objects, nil
}

// ----------------------------------------------------------------------
// Catalog
// ----------------------------------------------------------------------

type Catalog struct {
	db      *sql.DB
	Paths   []string        `json:"paths"`
	Lenses  NamedObjectList `json:"lenses"`
	Cameras NamedObjectList `json:"cameras"`
	Stats   *Stats          `json:"stats"`
	Photos  []*PhotoRecord  `json:"photos"`
}

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

func NewCatalog() *Catalog {
	return &Catalog{
		Stats:   newStats(),
		Lenses:  NamedObjectList{},
		Cameras: NamedObjectList{},
	}
}

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

func (c *Catalog) Merge(other *Catalog) {
	if other == nil {
		return
	}
	c.Paths = append(c.Paths, other.Paths...)
	c.Stats.Merge(other.Stats)
	c.Cameras = c.Cameras.Merge(other.Cameras)
	c.Lenses = c.Lenses.Merge(other.Lenses)
	c.Photos = append(c.Photos, other.Photos...)
}

func (c *Catalog) Close() error {
	return c.db.Close()
}

func (c *Catalog) GetLenses() (NamedObjectList, error) {
	return c.queryNamedObjects("select id_local, value from AgInternedExifLens")
}

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
          Lens.value as Lens,
          Camera.Value as Camera,
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
JOIN      AgLibraryIPTC             iptc       ON      image.id_local =     iptc.image
JOIN      AgLibraryFile             rootFile   ON   rootfile.id_local =    image.rootFile
JOIN      AgLibraryFolder           folder     ON     folder.id_local = rootfile.folder
JOIN      AgLibraryRootFolder       rootFolder ON rootFolder.id_local =   folder.rootFolder
JOIN      AgharvestedExifMetadata   exif       ON      image.id_local =     exif.image
LEFT JOIN AgInternedExifLens        Lens       ON       Lens.id_Local =     exif.lensRef
LEFT JOIN AgInternedExifCameraModel Camera     ON     Camera.id_local =     exif.cameraModelRef
`
	kPhotoRecordListOrderBy = "ORDER BY FullName"
)

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
	var capTime string
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

	p.CaptureTime, err = parseTime(capTime)
	if err != nil {
		return err
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

func (c *Catalog) GetPhotoCount() (int64, error) {
	row := c.db.QueryRow("select count(*) " + kPhotoRecordFrom)
	var count int64 = -1
	err := row.Scan(&count)
	return count, err
}

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

func (c *Catalog) GetPhotoByFilename(fn string) (*PhotoRecord, error) {
	return nil, nil
}

func (c *Catalog) GetPhotoByID(id int64) (*PhotoRecord, error) {
	return nil, nil
}

// ----------------------------------------------------------------------
// Sidecar Files
// ----------------------------------------------------------------------

const (
	sidecarColumns = `
select
      root.absolutePath
    , folder.pathFromRoot
    , file.baseName
    , file.extension
    , file.sidecarExtensions
`
	sidecarFrom = `
from        AgLibraryFile           as file

inner join  Adobe_images            as image
on          file.id_local = image.rootFile

inner join  AgLibraryFolder         as folder
on          file.folder = folder.id_local

inner join  AgLibraryRootFolder     as root
on          folder.rootFolder = root.id_local

where       file.sidecarExtensions  = 'JPG'
and         image.fileFormat        = 'RAW'
`
)

type SidecarFileStats struct {
	Count          uint
	TotalSizeBytes int64
}

type SidecarFileRecord struct {
	RootPath         string
	FilePath         string
	FileName         string
	Extension        string
	SidecarExtension string
	SidecarPath      string
	OriginalPath     string
}

func (c *Catalog) GetSidecarCount() (int, error) {
	row := c.db.QueryRow("select count(*) " + sidecarFrom)
	count := -1
	err := row.Scan(&count)
	return count, err
}

func (c *Catalog) GetSidecarFileStats() (*SidecarFileStats, error) {
	var count uint
	var size int64

	err := c.ForEachSidecar(func(record *SidecarFileRecord) error {
		if file, err := os.Open(record.SidecarPath); err == nil {
			if info, err := file.Stat(); err == nil {
				size += info.Size()
				count++
			}
		}
		return nil
	})

	return &SidecarFileStats{Count: count, TotalSizeBytes: size}, err
}

func (c *Catalog) ForEachSidecar(handler func(*SidecarFileRecord) error) error {
	rows, err := c.db.Query(sidecarColumns + sidecarFrom)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r := &SidecarFileRecord{}
		err = rows.Scan(&r.RootPath, &r.FilePath, &r.FileName, &r.Extension, &r.SidecarExtension)
		if err != nil {
			return err
		}
		r.SidecarPath = fmt.Sprintf("%s%s%s.%s",
			r.RootPath, r.FilePath, r.FileName, r.SidecarExtension)
		r.OriginalPath = fmt.Sprintf("%s%s%s.%s",
			r.RootPath, r.FilePath, r.FileName, r.Extension)

		handler(r)
	}
	return nil
}
