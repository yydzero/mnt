package reflect_test

import (

	. "github.com/onsi/ginkgo"
	"github.com/yydzero/mnt/util/reflect"
	"encoding/json"
)

type SegmentConfiguration struct {
	Id 			  int
	Data    	map[string]string
	//	Master        Segment
	//	StandbyMaster Segment
	Segments      map[string]Segment	`json:"segments"`
}

// Segment represent information about one segment
// Retrieved from gp_segment_configuration table.
type Segment struct {
	Dbid             int64		`json:"dbid"`
	Content          int64		`json:"content"`
	Role             string		`json:"role"`
	Preferred_role   string		`json:"preferred_role"`
	Mode             string		`json:"mode"`
	Status           string		`json:"status"`
	Port             int64		`json:"port"`
	Hostname         string		`json:"hostname"`
	Address          string		`json:"address"`
	Replication_port int64		`json:"replication_port"`
}


var _ = Describe("Reflect", func() {
	Describe("PrintVarInJson should print correct result", func() {
		It("Works for map", func() {
			v := make(map[string]int)
			v["Beijing"] = 100
			v["Shanghai"] = 50
			reflect.PrintVarInJson(v)
		})

		It("Works for struct", func() {
			sc := SegmentConfiguration{
				Id: 100,
				Segments: make(map[string]Segment),
			}

			segment := Segment{
				Dbid: 100,
				Content: -1,
				Hostname: "localhost",
			}
			sc.Segments[string(segment.Dbid)] = segment

			segment = Segment{
				Dbid: 101,
				Content: 0,
				Hostname: "localhost",
			}
			sc.Segments["000"] = segment

			json.Marshal(sc)
		})
	})
})
