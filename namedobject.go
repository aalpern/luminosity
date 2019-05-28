package luminosity

import (
	"sort"
)

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
