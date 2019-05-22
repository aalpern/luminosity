package luminosity

import (
	"fmt"
	"os"
)

const (
	sidecarColumns = `
select
      image.id_local as id
    , root.absolutePath
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

// SidecarFileStats records summary information about the sidecars of
// a catalog, including total count, total size on disk, and how many
// sidecars or original photos are missing.
type SidecarFileStats struct {
	// Count indicates the total number of sidecar files in a catalog.
	Count uint
	// MissingSidecarCount indicates the total number of sidecar files
	// which are missing on disk.
	MissingSidecarCount uint
	// MissingOriginalCount indicates the total number of files for
	// which a sidecar record exists, but the original file is missing
	// on disk.
	MissingOriginalCount uint
	// TotalSizeBytes indicates the total size in bytes of the sidecar
	// files on disk.
	TotalSizeBytes int64
}

type SidecarFileRecord struct {
	PhotoId          string
	RootPath         string
	FilePath         string
	FileName         string
	Extension        string
	SidecarExtension string
	// Absolute path to the sidecar file. Reconstructed from RootPath
	// + FilePath + FileName + SidecarExtension.
	SidecarPath string
	// Absolute path to the original photo file the sidecar is
	// associated with. Reconstructed from RootPath + FilePath +
	// FileName + Extension.
	OriginalPath string
}

func (c *Catalog) GetSidecarCount() (int, error) {
	row := c.db.QueryRow("select count(*) " + sidecarFrom)
	count := -1
	err := row.Scan(&count)
	return count, err
}

func (c *Catalog) GetSidecarFileStats() (*SidecarFileStats, error) {
	var count, missingSidecars, missingOriginals uint
	var size int64

	err := c.ForEachSidecar(func(record *SidecarFileRecord) error {
		if file, err := os.Open(record.OriginalPath); err != nil {
			if os.IsNotExist(err) {
				missingOriginals++
			}
		} else {
			file.Close()
		}

		if file, err := os.Open(record.SidecarPath); err != nil {
			if os.IsNotExist(err) {
				missingSidecars++
			}
		} else {
			if info, err := file.Stat(); err == nil {
				size += info.Size()
				count++
			}
			file.Close()
		}
		return nil
	})

	return &SidecarFileStats{
		Count:                count,
		MissingSidecarCount:  missingSidecars,
		MissingOriginalCount: missingOriginals,
		TotalSizeBytes:       size,
	}, err
}

func (c *Catalog) ForEachSidecar(handler func(*SidecarFileRecord) error) error {
	rows, err := c.query("for_each_sidecar", sidecarColumns+sidecarFrom)
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
