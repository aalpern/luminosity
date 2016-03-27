package luminosity

import (
	"fmt"
	"math"
)

// ApertureToFNumber converts an APEX aperture value to the more
// familiar expression of aperture as an f-number.
func ApertureToFNumber(a float64) float64 {
	return math.Exp2(a / 2)
}

// ShutterSpeedToExposureTime converts an APEX exposure time value to
// the conventional expression of exposure time in fractions of a
// second.
func ShutterSpeedToExposureTime(a float64) string {
	exposure := math.Exp2(a)
	return fmt.Sprintf("1/%.0f", exposure)
}
