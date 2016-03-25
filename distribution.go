package luminosity

import (
	"database/sql"
	"fmt"
	"sort"

	"gopkg.in/guregu/null.v3"
)

const (
	DayFormat = "2006-01-02"
)

// ----------------------------------------------------------------------
// Distibution Types & Utils
// ----------------------------------------------------------------------

type DistributionEntry struct {
	Id    int64  `json:"id"`
	Label string `json:"label"`
	Count int64  `json:"count"`
}

type DistributionList []*DistributionEntry
type DistributionMap map[string]*DistributionEntry

func (l DistributionList) ToMap() (m DistributionMap) {
	for _, d := range l {
		m[d.Label] = copyDistributionEntry(d)
	}
	return m
}

func (m DistributionMap) ToList() (d DistributionList) {
	for _, e := range m {
		d = append(d, e)
	}
	return d
}

func copyDistributionEntry(d *DistributionEntry) *DistributionEntry {
	return &DistributionEntry{
		Id:    d.Id,
		Count: d.Count,
		Label: d.Label,
	}
}

func MergeDistributions(dists ...DistributionList) DistributionList {
	merged := DistributionMap{}
	for _, dist := range dists {
		for _, entry := range dist {
			if target, ok := merged[entry.Label]; ok {
				target.Count = target.Count + entry.Count
			} else {
				merged[entry.Label] = copyDistributionEntry(entry)
			}
		}
	}
	list := merged.ToList()
	sort.Sort(list)
	return list
}

func (dl DistributionList) Merge(dists ...DistributionList) DistributionList {
	return MergeDistributions(append(dists, dl)...)
}

func (a DistributionList) Len() int           { return len(a) }
func (a DistributionList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DistributionList) Less(i, j int) bool { return a[i].Label < a[j].Label }

// ----------------------------------------------------------------------
// Distribution Queries
// ----------------------------------------------------------------------

func (c *Catalog) GetPhotoCountsByDate() (DistributionList, error) {
	const query = `
SELECT 0,
       date(captureTime),
       count(*)
FROM   Adobe_images
GROUP  BY date(captureTime)
ORDER  BY date(captureTime)
`
	return c.queryDistribution(query, defaultDistributionConvertor)
}

type ByDate DistributionList

func (a ByDate) Len() int      { return len(a) }
func (a ByDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool {

	return a[i].Label < a[j].Label
}

func (c *Catalog) GetLensDistribution() (DistributionList, error) {
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

func (c *Catalog) GetFocalLengthDistribution() (DistributionList, error) {
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

func (c *Catalog) GetCameraDistribution() (DistributionList, error) {
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

func (c *Catalog) GetApertureDistribution() (DistributionList, error) {
	const query = `
SELECT   aperture,
         count(aperture)
FROM     AgHarvestedExifMetadata
WHERE    aperture is not null
GROUP BY aperture
ORDER BY aperture
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

func (c *Catalog) GetExposureTimeDistribution() (DistributionList, error) {
	const query = `
SELECT   shutterSpeed,
         count(shutterSpeed)
FROM     AgHarvestedExifMetadata
WHERE    shutterSpeed is not null
GROUP BY shutterSpeed
ORDER BY shutterSpeed
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
	var label null.String
	var id, count int64
	if err := rows.Scan(&id, &label, &count); err != nil {
		return nil, err
	}
	return &DistributionEntry{
		Id:    id,
		Label: label.String,
		Count: count,
	}, nil
}

func (c *Catalog) queryDistribution(sql string, fn distributionConvertor) (DistributionList, error) {
	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return convertDistribution(rows, fn)
}

func convertDistribution(rows *sql.Rows, fn distributionConvertor) (DistributionList, error) {
	var entries DistributionList
	for rows.Next() {
		if entry, err := fn(rows); err != nil {
			return nil, err
		} else {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

// ----------------------------------------------------------------------
// Starburst Stats
// ----------------------------------------------------------------------

const query = `
SELECT    count(*)          as count,
          image.id_local    as id,
          Camera.Value      as Camera,
          Lens.value        as Lens,
          exif.aperture     as Aperture,
          exif.focalLength  as FocalLength
FROM      Adobe_images              image
JOIN      AgharvestedExifMetadata   exif      ON  image.id_local  = exif.image
LEFT JOIN AgInternedExifLens        Lens      ON  Lens.id_Local   = exif.lensRef
LEFT JOIN AgInternedExifCameraModel Camera    ON  Camera.id_local = exif.cameraModelRef
WHERE camera is not null and lens is not null
GROUP BY Camera, Lens, Aperture, FocalLength
ORDER BY Camera, Lens, Aperture, FocalLength, count
`

// ----------------------------------------------------------------------
// Composite Stats Object
// ----------------------------------------------------------------------

type Stats struct {
	ByDate         DistributionList `json:"by_date"`
	ByCamera       DistributionList `json:"by_camera"`
	ByLens         DistributionList `json:"by_lens"`
	ByFocalLength  DistributionList `json:"by_focal_length"`
	ByAperture     DistributionList `json:"by_aperture"`
	ByExposureTime DistributionList `json:"by_exposure_time"`
}

func newStats() *Stats {
	return &Stats{
		ByDate:         DistributionList{},
		ByCamera:       DistributionList{},
		ByLens:         DistributionList{},
		ByFocalLength:  DistributionList{},
		ByAperture:     DistributionList{},
		ByExposureTime: DistributionList{},
	}
}

func (s *Stats) Merge(other *Stats) {
	s.ByDate = s.ByDate.Merge(other.ByDate)
	s.ByCamera = s.ByCamera.Merge(other.ByCamera)
	s.ByLens = s.ByLens.Merge(other.ByLens)
	s.ByFocalLength = s.ByFocalLength.Merge(other.ByFocalLength)
	s.ByAperture = s.ByAperture.Merge(other.ByAperture)
	s.ByExposureTime = s.ByExposureTime.Merge(other.ByExposureTime)

	sort.Sort(ByDate(s.ByDate))
}

func (c *Catalog) GetStats() (*Stats, error) {
	s := &Stats{}

	if d, err := c.GetPhotoCountsByDate(); err != nil {
		return nil, err
	} else {
		s.ByDate = d
	}

	if d, err := c.GetCameraDistribution(); err != nil {
		return nil, err
	} else {
		s.ByCamera = d
	}

	if d, err := c.GetLensDistribution(); err != nil {
		return nil, err
	} else {
		s.ByLens = d
	}

	if d, err := c.GetFocalLengthDistribution(); err != nil {
		return nil, err
	} else {
		s.ByFocalLength = d
	}

	if d, err := c.GetApertureDistribution(); err != nil {
		return nil, err
	} else {
		s.ByAperture = d
	}

	if d, err := c.GetExposureTimeDistribution(); err != nil {
		return nil, err
	} else {
		s.ByExposureTime = d
	}

	return s, nil
}
