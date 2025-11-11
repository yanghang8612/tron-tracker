package common

import (
	"fmt"
	"testing"
)

func TestFormatWithUnits(t *testing.T) {
	fmt.Println(FormatWithUnits(547_547_547_547))
	fmt.Println(FormatWithUnits(47_547_547_547))
	fmt.Println(FormatWithUnits(7_547_547_547))
	fmt.Println(FormatWithUnits(547_547_547))
	fmt.Println(FormatWithUnits(47_547_547))
	fmt.Println(FormatWithUnits(7_547_547))
	fmt.Println(FormatWithUnits(547_547))
	fmt.Println(FormatWithUnits(47_547))
	fmt.Println(FormatWithUnits(7_547))
	fmt.Println(FormatWithUnits(547))
	fmt.Println(FormatWithUnits(47))
	fmt.Println(FormatWithUnits(7))
}
