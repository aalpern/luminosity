package luminosity

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	null "gopkg.in/guregu/null.v3"
)

const (
	kPhotoRecordSelect = `
SELECT    image.id_local,
          image.id_global,
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
          iptc.copyright,
          coalesce(Creator.value, 'Unknown') as creator
`
	kPhotoRecordFrom = `
FROM      Adobe_images              image
JOIN      AgLibraryFile             rootFile   ON   rootfile.id_local = image.rootFile
JOIN      AgLibraryFolder           folder     ON     folder.id_local = rootfile.folder
JOIN      AgLibraryRootFolder       rootFolder ON rootFolder.id_local = folder.rootFolder
LEFT JOIN AgLibraryIPTC             iptc       ON      image.id_local = iptc.image
LEFT JOIN AgharvestedExifMetadata   exif       ON      image.id_local = exif.image
LEFT JOIN AgInternedExifLens        Lens       ON       Lens.id_Local = exif.lensRef
LEFT JOIN AgInternedExifCameraModel Camera     ON     Camera.id_local = exif.cameraModelRef
LEFT JOIN AgInternedIptcCreator     Creator    ON    Creator.id_local = iptc.image
`
	kPhotoRecordListOrderBy = "ORDER BY FullName"
)

// PhotoRecord gathers the most commonly used information about each
// photo into a single record, extracted from 8 different tables in
// the Lightroom catalog.
type PhotoRecord struct {
	Id       int         `json:"id"`
	IdGlobal string      `json:"id_global"`
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
	Creator   null.String `json:"creator"`

	// Pointer back to the catalog that contains this record
	Catalog *Catalog `json:"-"`
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

	err := row.Scan(
		&p.Id, &p.IdGlobal, &p.FullName, &p.Lens, &p.Camera,
		// Image
		&p.FileFormat, &p.FileHeight, &p.FileWidth, &p.Orientation, &capTime, &p.Rating, &p.ColorLabels, &p.Pick,
		// Exif
		&p.DateDay, &p.DateMonth, &p.DateYear, &p.FlashFired, &p.ISO, &shutterSpeedString, &p.FocalLength, &apertureString,
		&p.HasGPS, &p.Latitude, &p.Longitude,
		// Iptc
		&p.Caption, &p.Copyright, &p.Creator,
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

// GetPreview returns the highest resolution preview available for the
// given photo, if one exists.
func (p *PhotoRecord) GetPreview() ([]byte, error) {
	previews, err := p.Catalog.Previews()
	if err != nil {
		return nil, err
	}

	ci, err := previews.GetPhotoCacheInfo(p)
	if err != nil {
		return nil, err
	}

	pf, err := OpenPreviewFile(ci.Path())
	return pf.Sections[len(pf.Sections)-1].ReadData()
}

// ForEachPhoto takes a handler function and calls it successively on
// a PhotoRecord structure for every photo in the catalog. Returning
// an error from the handler function will stop the iteration.
func (c *Catalog) ForEachPhoto(handler func(*PhotoRecord) error) error {
	if photos, err := c.GetPhotos(); err != nil {
		return err
	} else {
		for _, photo := range photos {
			if err = handler(photo); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetPhotoCount returns a simple count of the total number of images
// stored in the catalog.
func (c *Catalog) GetPhotoCount() (int64, error) {
	row := c.db.queryRow("get_photo_count", "select count(*) "+kPhotoRecordFrom)
	var count int64 = -1
	err := row.Scan(&count)
	return count, err
}

// GetPhotos returns an array of PhotoRecord structs for every photo
// represented in the catalog.
func (c *Catalog) GetPhotos() ([]*PhotoRecord, error) {
	if c.Photos != nil {
		return c.Photos, nil
	}
	count, err := c.GetPhotoCount()
	if err != nil {
		return nil, err
	}
	rows, err := c.db.query("get_photos",
		kPhotoRecordSelect+
			kPhotoRecordFrom+
			kPhotoRecordListOrderBy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	photos := make([]*PhotoRecord, 0, count)
	for rows.Next() {
		p := &PhotoRecord{
			Catalog: c,
		}
		err = p.scan(rows)
		if err != nil {
			return photos, err
		}
		photos = append(photos, p)
	}
	return photos, nil
}
