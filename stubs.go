package dukedb

import (
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"
)

func PickRandom(slice interface{}) interface{} {
	sliceVal := reflect.ValueOf(slice)
	if sliceVal.Type().Kind() != reflect.Slice {
		panic("Expected a slice")
	}

	itemCount := sliceVal.Len()
	index := rand.Intn(itemCount - 1)

	return sliceVal.Index(index).Interface()
}

func PickRandomSubSlice(slice interface{}, count int) interface{} {
	sliceVal := reflect.ValueOf(slice)
	if sliceVal.Type().Kind() != reflect.Slice {
		panic("Expected a slice")
	}

	itemCount := sliceVal.Len()

	newSlice := NewSlice(sliceVal.Type().Elem())
	newSliceVal := reflect.ValueOf(newSlice)

	used := make(map[int]bool)

	for i := 0; i < count; i++ {
		index := -1
		for {
			index = rand.Intn(itemCount - 1)
			if _, ok := used[index]; !ok {
				break
			}
		}

		used[index] = true
		newSliceVal = reflect.Append(newSliceVal, sliceVal.Index(index))
	}

	return newSliceVal.Interface()
}

func RandomBool() bool {
	i := rand.Intn(1)
	return i == 1
}

func RandomInt(min, max int) int {
	return rand.Intn(max-min) + min
}

func RandomTime(min, max time.Time) time.Time {
	diff := max.Sub(min)
	return min.Add(time.Duration(RandomInt(0, int(diff))))
}

func StubText(length int) string {
	placeholder := `Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`

	repeat := int(math.Ceil(float64(length) / float64(len(placeholder))))
	text := strings.Repeat(placeholder, repeat)[:length]

	return text
}

func StubTextRandom(min, max int) string {
	return StubText(RandomInt(min, max))
}
