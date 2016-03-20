package luminosity

import (
	"database/sql"
	"fmt"
	"sort"
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

func distributionListToMap(dist []*DistributionEntry) (m map[string]*DistributionEntry) {
	for _, d := range dist {
		m[d.Label] = copyDistributionEntry(d)
	}
	return m
}

func mapToDistributionList(m map[string]*DistributionEntry) (d []*DistributionEntry) {
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

func MergeDistributions(dists ...[]*DistributionEntry) []*DistributionEntry {
	merged := map[string]*DistributionEntry{}
	for _, dist := range dists {
		for _, entry := range dist {
			if target, ok := merged[entry.Label]; ok {
				target.Count = target.Count + entry.Count
			} else {
				merged[entry.Label] = copyDistributionEntry(entry)
			}
		}
	}
	list := mapToDistributionList(merged)
	sort.Sort(ByLabel(list))
	return list
}

type ByLabel []*DistributionEntry

func (a ByLabel) Len() int           { return len(a) }
func (a ByLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLabel) Less(i, j int) bool { return a[i].Label < a[j].Label }

// ----------------------------------------------------------------------
// Distribution Queries
// ----------------------------------------------------------------------

func (c *Catalog) GetPhotoCountsByDate() ([]*DistributionEntry, error) {
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

func (c *Catalog) GetExposureTimeDistribution() ([]*DistributionEntry, error) {
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
