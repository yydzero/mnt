package reflect

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"os/user"
)

// PrintVarInJson prints given variable in indented JSON format.
// As JSON only allows string key, so map with other type of keys are not displayed.
func PrintVarInJson(v interface{}) {
	fmt.Printf("%s\n", ToJson(v))
}

func ToJson(v interface{}) string {
	b, _ := json.Marshal(v)
	var out bytes.Buffer
	json.Indent(&out, b, "", "\t")

	return out.String()
}

func PrintType(v interface{}) {
	fmt.Printf("%s\n", reflect.TypeOf(v))
}

// Return the current directory as calling method's file
func GetCurrentDir() string {
	_, myfilename, _, _ := runtime.Caller(1)
	return path.Dir(myfilename)
}

func GetCurrentUsername() string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return u.Username
}

func ToString(v interface{}) string {
//	switch v.(type) {
//		case int:
//		return strconv.Itoa(v)
//		case int64:
//		return strconv.FormatInt(v, 10)
//		case uint64:
//		return strconv.FormatUint(v, 10)
//		case float32:
//		return strconv.FormatFloat(v, 'f', -1, 32)
//		case float64:
//		return strconv.FormatFloat(v, 'f', -1, 64)
//		case bool:
//		return strconv.FormatBool(v)
//		default:
//		return v.(string)
//	}
	return fmt.Sprintf("%v", v)
}
