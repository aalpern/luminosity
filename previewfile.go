package luminosity

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	errorNotEnoughBytesForMarker = fmt.Errorf("Not enough bytes for marker")
	errorUnknownMarker           = fmt.Errorf("Unknown marker")
)

const (
	PreviewHeaderMarker = "AgHg"
)

type PreviewHeaderFixed struct {
	// HeaderLength includes the size of the 'AgHg' marker and the 2
	// bytes for Headerlength itself
	HeaderLength uint16 // 2
	Version      uint8  // 1
	Kind         uint8  // 1
	Length       uint64 // 8
	Padding      uint64 // 8
}

// PreviewHeader represents the header sections for the different
// preview resolutions embedded in a .lrprev file.
type PreviewHeader struct {
	PreviewHeaderFixed
	DataOffset int64
	Name       string
}

func (h PreviewHeader) NameLength() int {
	return int(h.HeaderLength) - 24
}

func readMarker(f *os.File) error {
	marker := make([]byte, 4)
	read, err := f.Read(marker)
	if err != nil {
		return err
	}
	if read < 4 {
		return errorNotEnoughBytesForMarker
	}
	if string(marker) != "AgHg" {
		return errorUnknownMarker
	}
	return nil
}

func readHeader(f *os.File) (*PreviewHeader, error) {
	if err := readMarker(f); err != nil {
		return nil, err
	}

	var header PreviewHeader
	if err := binary.Read(f, binary.BigEndian, &header.PreviewHeaderFixed); err != nil {
		return nil, err
	}

	name := make([]byte, header.NameLength())
	if _, err := f.Read(name); err != nil {
		return nil, err
	} else {
		header.Name = strings.Split(string(name), "\u0000")[0]
	}

	offset, _ := f.Seek(0, io.SeekCurrent)
	header.DataOffset = offset

	_, err := f.Seek(int64(header.Length+header.Padding), io.SeekCurrent)
	if err != nil {
		return &header, err
	}

	return &header, nil
}

func ReadPreviewHeaders(f *os.File) ([]*PreviewHeader, error) {
	f.Seek(0, io.SeekStart)

	headers := make([]*PreviewHeader, 0, 8)

	header, err := readHeader(f)
	for ; err == nil; header, err = readHeader(f) {
		headers = append(headers, header)
	}
	if err == io.EOF {
		err = nil
	}
	return headers, err
}
