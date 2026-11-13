package sdkgen_test

import (
	"testing"

	"github.com/donutnomad/wlang/go2wlang"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// 与下面 wireProbe 源码同形的 Go 类型：类型名不同，wire 指纹必须一致。
type wireTree struct {
	Label    string     `json:"label"`
	Children []wireTree `json:"children"`
	Parent   *wireTree  `json:"parent"`
	Tags     wireTags   `json:"tags"`
	Owner    *wireOwner `json:"owner"`
}

type wireTags []string

type wireOwner struct {
	Name  string              `json:"name"`
	Trees map[string]wireTree `json:"trees"`
}

const wireProbe = `package probe

type Node struct {
	Label    string          ` + "`json:\"label\"`" + `
	Children []Node          ` + "`json:\"children\"`" + `
	Parent   *Node           ` + "`json:\"parent\"`" + `
	Tags     Labels          ` + "`json:\"tags\"`" + `
	Owner    *Keeper         ` + "`json:\"owner\"`" + `
}

type Labels []string

type Keeper struct {
	Name  string          ` + "`json:\"name\"`" + `
	Trees map[string]Node ` + "`json:\"trees\"`" + `
}

func Probe(_ any, in Node) (Keeper, error) {
	return Keeper{}, nil
}
`

// 递归、互相递归与具名 slice 类型的 wire 指纹：Workflow 侧从描述符算，Worker 侧从 Go 类型算，两侧一致，
// Activity 契约对照才能通过。
func TestWireFingerprintOfRecursiveTypesMatchesDescriptors(t *testing.T) {
	translated, err := go2wlang.TranslateFileDetailed([]byte(wireProbe), go2wlang.Options{FuncName: "Probe"})
	if err != nil {
		t.Fatal(err)
	}
	for _, side := range []struct {
		id     string
		goSide func() (string, error)
	}{
		{translated.Entry.Input, activity.WireFingerprintOf[wireTree]},
		{translated.Entry.Output, activity.WireFingerprintOf[wireOwner]},
	} {
		fromDescriptors, err := program.WLangWireFingerprint(side.id, translated.Types)
		if err != nil {
			t.Fatalf("%s: %v", side.id, err)
		}
		fromGo, err := side.goSide()
		if err != nil {
			t.Fatalf("%s Go side: %v", side.id, err)
		}
		if fromDescriptors != fromGo {
			t.Fatalf("%s: descriptor fingerprint %s, Go fingerprint %s", side.id, fromDescriptors, fromGo)
		}
	}
}
