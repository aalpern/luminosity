package luminosity

import (
	"sort"
)

type Stats struct {
	ByDate         DistributionList `json:"by_date"`
	ByCamera       DistributionList `json:"by_camera"`
	ByLens         DistributionList `json:"by_lens"`
	ByFocalLength  DistributionList `json:"by_focal_length"`
	ByAperture     DistributionList `json:"by_aperture"`
	ByExposureTime DistributionList `json:"by_exposure_time"`
	ByEditCount    DistributionList `json:"by_edit_count"`
	ByKeyword      DistributionList `json:"by_keyword"`
}

func newStats() *Stats {
	return &Stats{
		ByDate:         DistributionList{},
		ByCamera:       DistributionList{},
		ByLens:         DistributionList{},
		ByFocalLength:  DistributionList{},
		ByAperture:     DistributionList{},
		ByExposureTime: DistributionList{},
		ByEditCount:    DistributionList{},
		ByKeyword:      DistributionList{},
	}
}

func (s *Stats) Merge(other *Stats) {
	s.ByDate = s.ByDate.Merge(other.ByDate)
	s.ByCamera = s.ByCamera.Merge(other.ByCamera)
	s.ByLens = s.ByLens.Merge(other.ByLens)
	s.ByFocalLength = s.ByFocalLength.Merge(other.ByFocalLength)
	s.ByAperture = s.ByAperture.Merge(other.ByAperture)
	s.ByExposureTime = s.ByExposureTime.Merge(other.ByExposureTime)
	s.ByEditCount = s.ByEditCount.Merge(other.ByEditCount)
	s.ByKeyword = s.ByKeyword.Merge(other.ByKeyword)

	sort.Sort(ByDate(s.ByDate))
}

func (c *Catalog) GetStats() (*Stats, error) {
	if c.Stats != nil {
		return c.Stats, nil
	}

	s := newStats()

	if c.db == nil {
		c.Stats = s
		return s, nil
	}

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

	if d, err := c.GetEditCountDistribution(); err != nil {
		return nil, err
	} else {
		s.ByEditCount = d
	}

	if d, err := c.GetKeywordDistribution(); err != nil {
		return nil, err
	} else {
		s.ByKeyword = d
	}

	c.Stats = s
	return c.Stats, nil
}
