package luminosity

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/guregu/null.v3"
)

type Catalog struct {
	db   *sql.DB
	path string
}

func OpenCatalog(path string) (*Catalog, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &Catalog{
		db:   db,
		path: path,
	}, nil
}

type NamedObject struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

func (c *Catalog) Close() error {
	return c.db.Close()
}

func (c *Catalog) GetLenses() ([]*NamedObject, error) {
	return c.queryNamedObjects("select id_local, value from AgInternedExifLens")
}

func (c *Catalog) GetCameras() ([]*NamedObject, error) {
	return c.queryNamedObjects("select id_local, value from AgInternedExifCameraModel")
}

func (c *Catalog) queryNamedObjects(sql string) ([]*NamedObject, error) {
	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return convertNamedObjects(rows)
}

func convertNamedObjects(rows *sql.Rows) ([]*NamedObject, error) {
	var objects []*NamedObject
	for rows.Next() {
		obj := &NamedObject{}
		if err := rows.Scan(&obj.Id, &obj.Name); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, nil
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

	p.CaptureTime, err = time.Parse("2006-01-02T15:04:05", capTime)
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
	// TODO - get count first to set photos capacity
	var photos []*PhotoRecord
	err := c.ForEachPhoto(func(p *PhotoRecord) error {
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
// Stats
// ----------------------------------------------------------------------

type DistributionEntry struct {
	Id    int64  `json:"id"`
	Label string `json:"label"`
	Count int64  `json:"count"`
}

func (c *Catalog) GetLensDistribution() ([]*DistributionEntry, error) {
	const query = `
SELECT    LensRef.id_local      as id,
          LensRef.value         as name,
          count(LensRef.value)  as count

FROM      Adobe_images               image
JOIN      AgharvestedExifMetadata    metadata   ON       image.id_local = metadata.image
LEFT JOIN AgInternedExifLens         LensRef    ON     LensRef.id_local = metadata.lensRef
WHERE     id is not null
GROUP BY  id
ORDER BY  count desc
`
	return c.queryDistribution(query, defaultDistributionConvertor)
}

func (c *Catalog) GetFocalLengthDistribution() ([]*DistributionEntry, error) {
	const query = `
SELECT id_local          as id,
       focalLength       as name,
       count(id_local)   as count

FROM   AgHarvestedExifMetadata
WHERE       focalLength is not null
GROUP BY    focalLength
ORDER BY    count DESC
`
	return c.queryDistribution(query, defaultDistributionConvertor)
}

func (c *Catalog) GetCameraDistribution() ([]*DistributionEntry, error) {
	const query = `
SELECT    Camera.id_local       as id,
          Camera.value          as name,
          count(Camera.value)   as count

FROM      Adobe_images               image
JOIN      AgharvestedExifMetadata    metadata   ON      image.id_local = metadata.image
LEFT JOIN AgInternedExifCameraModel  Camera     ON     Camera.id_local = metadata.cameraModelRef
WHERE     id is not null
GROUP BY  id
ORDER BY  count desc
`
	return c.queryDistribution(query, defaultDistributionConvertor)
}

func (c *Catalog) GetApertureDistribution() ([]*DistributionEntry, error) {
	const query = `
SELECT   aperture,
         count(aperture)
FROM     AgHarvestedExifMetadata
WHERE    aperture is not null
GROUP BY aperture
`
	return c.queryDistribution(query, func(row *sql.Rows) (*DistributionEntry, error) {
		var aperture float64
		var count int64
		if err := row.Scan(&aperture, &count); err != nil {
			return nil, err
		}
		return &DistributionEntry{
			Label: fmt.Sprintf("%.1f", ApertureToFNumber(aperture)),
			Count: count,
		}, nil
	})
}

func (c *Catalog) GetExposureTimeDistribution() ([]*DistributionEntry, error) {
	const query = `
select shutterSpeed, count(*)
from AgHarvestedExifMetadata
where shutterSpeed is not null
group by shutterSpeed
order by shutterSpeed
`
	return c.queryDistribution(query, func(row *sql.Rows) (*DistributionEntry, error) {
		var shutter float64
		var count int64
		if err := row.Scan(&shutter, &count); err != nil {
			return nil, err
		}
		return &DistributionEntry{
			Label: ShutterSpeedToExposureTime(shutter),
			Count: count,
		}, nil
	})
}

type distributionConvertor func(*sql.Rows) (*DistributionEntry, error)

func defaultDistributionConvertor(rows *sql.Rows) (*DistributionEntry, error) {
	var label string
	var id, count int64
	if err := rows.Scan(&id, &label, &count); err != nil {
		return nil, err
	}
	return &DistributionEntry{
		Id:    id,
		Label: label,
		Count: count,
	}, nil
}

func (c *Catalog) queryDistribution(sql string, fn distributionConvertor) ([]*DistributionEntry, error) {
	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return convertDistribution(rows, fn)
}

func convertDistribution(rows *sql.Rows, fn distributionConvertor) ([]*DistributionEntry, error) {
	var entries []*DistributionEntry
	for rows.Next() {
		if entry, err := fn(rows); err != nil {
			return nil, err
		} else {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

type PhotoCountByDate struct {
	Count int64     `json:"count"`
	Date  time.Time `json:"date"`
}

func (p *PhotoCountByDate) scan(rows *sql.Rows) (err error) {
	var date string
	if err = rows.Scan(&p.Count, &date); err != nil {
		return err
	}
	p.Date, err = time.Parse("2006-01-02", date)
	if err != nil {
		return err
	}
	return nil
}

func (c *Catalog) GetPhotoCountsByDate() ([]*PhotoCountByDate, error) {
	const sql = `
select count(*) as count, date(captureTime) as date
from Adobe_images
group by date(captureTime)
order by date(captureTime)
`
	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var counts []*PhotoCountByDate
	for rows.Next() {
		entry := &PhotoCountByDate{}
		if err := entry.scan(rows); err != nil {
			return nil, err
		}
		counts = append(counts, entry)
	}
	return counts, nil
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
