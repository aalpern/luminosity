package luminosity

import (
	"fmt"
	"math"
)

func ApertureToFNumber(a float64) float64 {
	return math.Exp2(a / 2)
}

func ShutterSpeedToExposureTime(a float64) string {
	exposure := math.Exp2(a)
	return fmt.Sprintf("1/%.0f", exposure)
}
