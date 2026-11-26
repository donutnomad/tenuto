package sdkgen

import (
	"encoding/json"
	"strings"

	"github.com/donutnomad/wlang/go2wlang"
	wlangtypes "github.com/donutnomad/wlang/types"

	programpkg "github.com/donutnomad/tenuto/program"
)

// normalizeProgramTypes 在翻译之后整理程序与类型表，返回同样整理过的 extraTypes：
//
//   - 底层是基础类型的具名类型统一成底层类型（见 basicNamedTypes）；
//   - 程序里出现的 map<string,V>，V 是 slice 或 map 时补齐 V（及其内部集合）与这个 map 的描述符（见
//     collectionDescriptors）。
func normalizeProgramTypes(translated *go2wlang.Result, extraTypes map[string]programpkg.WLangTypeDescriptor) (map[string]programpkg.WLangTypeDescriptor, error) {
	var raw map[string]any
	if err := json.Unmarshal(translated.JSON, &raw); err != nil {
		return nil, err
	}
	program := raw["program"]
	named := newBasicNamedTypes(translated.Types, extraTypes)
	if len(named.ids) > 0 {
		translated.Types = named.descriptors(translated.Types)
		translated.Entry.Input = rename(translated.Entry.Input, named.ids)
		translated.Entry.Output = rename(translated.Entry.Output, named.ids)
		extraTypes = named.descriptors(extraTypes)
		program = mapTypeNames(program, func(name string) string { return rename(name, named.names) })
	}
	added := collectionDescriptors(program, translated.Types)
	if len(named.ids) == 0 && len(added) == 0 {
		return extraTypes, nil
	}
	if translated.Types == nil {
		translated.Types = make(map[string]programpkg.WLangTypeDescriptor, len(added))
	}
	for id, descriptor := range added {
		translated.Types[id] = descriptor
	}
	raw["program"] = program
	raw["types"] = translated.Types
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	translated.JSON = encoded
	return extraTypes, nil
}

// basicNamedTypes 把底层是基础类型的具名类型（`type Status string`、`type Prio int`）换成底层类型。
//
// go2wlang 给这类类型生成 alias 描述符：按描述符解码的载荷（Workflow 输入、Update 参数、Activity、子 Workflow 与
// Signal 结果）里的值带 alias 的类型名，而常量、字面量、`var s Status` 与函数形参都是底层类型。WLang 的 ==、
// switch 与有序比较、算术要求类型名相同，同一个 Go 类型的两种来源互相比较恒为不等或报 E_TYPE。生成器把类型表、
// 入口与程序里的这些类型名统一成底层类型，两种来源就是同一个类型；编码输出只看底层类型，载荷不变。
//
// 代价：放进 any 之后具名值与底层类型的值不再区分（`any(Status("a")) == "a"` 为 true，`x.(Status)` 与
// `x.(string)` 判断相同），go-subset 文档列为差异。
type basicNamedTypes struct {
	// ids 与 names 分别按描述符 ID、程序里的类型名映射到底层的基础类型名。
	ids, names map[string]string
}

// newBasicNamedTypes 从各组描述符里找出底层（经 alias 链）是基础标量类型的 alias。
func newBasicNamedTypes(tables ...map[string]programpkg.WLangTypeDescriptor) basicNamedTypes {
	all := map[string]programpkg.WLangTypeDescriptor{}
	for _, table := range tables {
		for id, descriptor := range table {
			all[id] = descriptor
		}
	}
	named := basicNamedTypes{ids: map[string]string{}, names: map[string]string{}}
	for id, descriptor := range all {
		if descriptor.Kind != wlangtypes.TypeKindAlias {
			continue
		}
		underlying := descriptor.Underlying
		for range len(all) {
			next, ok := all[underlying]
			if !ok || next.Kind != wlangtypes.TypeKindAlias {
				break
			}
			underlying = next.Underlying
		}
		if basicScalarType(underlying) {
			named.ids[id] = underlying
			named.names[descriptor.LanguageName] = underlying
		}
	}
	return named
}

func basicScalarType(id string) bool {
	switch id {
	case wlangtypes.TString, wlangtypes.TBoolean,
		wlangtypes.TInt8, wlangtypes.TInt16, wlangtypes.TInt32, wlangtypes.TInt64,
		wlangtypes.TUint8, wlangtypes.TUint16, wlangtypes.TUint32, wlangtypes.TUint64,
		wlangtypes.TFloat32, wlangtypes.TFloat64:
		return true
	}
	return false
}

// descriptors 返回去掉这些 alias、其余描述符里的引用换成底层类型后的类型表。改名后与已有描述符同 ID 的
// （`[]Status` 与 `[]string`）是同一个类型，保留一份。
func (n basicNamedTypes) descriptors(table map[string]programpkg.WLangTypeDescriptor) map[string]programpkg.WLangTypeDescriptor {
	if len(n.ids) == 0 || table == nil {
		return table
	}
	out := make(map[string]programpkg.WLangTypeDescriptor, len(table))
	for id, descriptor := range table {
		if _, ok := n.ids[id]; ok {
			continue
		}
		descriptor.ID = rename(id, n.ids)
		descriptor.LanguageName = rename(descriptor.LanguageName, n.names)
		descriptor.Underlying = rename(descriptor.Underlying, n.ids)
		descriptor.Key = rename(descriptor.Key, n.ids)
		descriptor.Elem = rename(descriptor.Elem, n.ids)
		descriptor.Fields = append([]wlangtypes.FieldDescriptor(nil), descriptor.Fields...)
		for i := range descriptor.Fields {
			descriptor.Fields[i].TypeID = rename(descriptor.Fields[i].TypeID, n.ids)
		}
		descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
		out[descriptor.ID] = descriptor
	}
	return out
}

// rename 把类型名 s 里作为完整名字出现的键换成对应的基础类型名：`flows.Status`、`array<flows.Status>`、
// `map<string,flows.Status>`、`[]flows.Status`、`*flows.Prio`。名字由字母、数字、`_`、`.`、`/`、`-` 组成，
// 其余字符是分隔符。
func rename(s string, names map[string]string) string {
	if s == "" || len(names) == 0 {
		return s
	}
	var out strings.Builder
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		word := s[start:end]
		if replaced, ok := names[word]; ok {
			word = replaced
		}
		out.WriteString(word)
		start = -1
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || c == '.' || c == '/' || c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 {
			if start < 0 {
				start = i
			}
			continue
		}
		flush(i)
		out.WriteByte(c)
	}
	flush(len(s))
	return out.String()
}

// collectionDescriptors 返回程序里 map<string,V>（V 是 slice 或 map）需要、类型表里还没有的描述符。
//
// WLang 的 map 字面量与零值只认得基础类型的值类型；值类型是集合时要按描述符建 map，描述符由 go2wlang 只为含
// 程序具名类型的形状生成。`map[string][]string{}`（按 key 分组）、`map[string][]any{}`、
// `map[string]map[string]int{}` 因此在运行期报 E_TYPE。这里按类型名补上 slice 与 map 的描述符；
// 元素里有类型表之外的类型（指针、channel、宿主类型）时不补。
func collectionDescriptors(program any, table map[string]programpkg.WLangTypeDescriptor) map[string]programpkg.WLangTypeDescriptor {
	byName := map[string]string{}
	for id, descriptor := range table {
		byName[id] = id
		byName[descriptor.LanguageName] = id
	}
	added := map[string]programpkg.WLangTypeDescriptor{}
	var ensure func(name string) string
	ensure = func(name string) string {
		if basicScalarType(name) || name == wlangtypes.TAny {
			return name
		}
		if id, ok := byName[name]; ok {
			return id
		}
		descriptor := programpkg.WLangTypeDescriptor{LanguageName: name}
		if elem, ok := strings.CutPrefix(name, "array<"); ok && strings.HasSuffix(elem, ">") {
			elemID := ensure(strings.TrimSuffix(elem, ">"))
			if elemID == "" {
				return ""
			}
			descriptor.ID, descriptor.Kind, descriptor.Elem = "[]"+elemID, wlangtypes.TypeKindSlice, elemID
		} else if value, ok := strings.CutPrefix(name, "map<string,"); ok && strings.HasSuffix(value, ">") {
			elemID := ensure(strings.TrimSuffix(value, ">"))
			if elemID == "" {
				return ""
			}
			descriptor.ID, descriptor.Kind, descriptor.Key, descriptor.Elem = "map[string]"+elemID, wlangtypes.TypeKindMap, wlangtypes.TString, elemID
		} else {
			return ""
		}
		if _, exists := table[descriptor.ID]; exists {
			byName[name] = descriptor.ID
			return descriptor.ID
		}
		descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
		added[descriptor.ID] = descriptor
		byName[name] = descriptor.ID
		return descriptor.ID
	}
	mapTypeNames(program, func(name string) string {
		if value, ok := strings.CutPrefix(name, "map<string,"); ok {
			value = strings.TrimSuffix(value, ">")
			if strings.HasPrefix(value, "array<") || strings.HasPrefix(value, "map<") {
				ensure(name)
			}
		}
		return name
	})
	return added
}

// mapTypeNames 用 fn 改写程序 JSON 里写类型名的位置：zero、函数形参与返回值、数组元素、map 与 chan 的类型、
// struct 名、let 的声明类型、字面量的类型，以及 type.assert、type.assert.ok、type.is 的目标与 ptr.new 的类型。
// map 的类型 [K, V] 另外以 map<K,V> 的整体名字交给 fn 看一遍（结果不用）。
func mapTypeNames(value any, fn func(string) string) any {
	switch current := value.(type) {
	case []any:
		for i, item := range current {
			current[i] = mapTypeNames(item, fn)
		}
		return current
	case map[string]any:
		for key, item := range current {
			current[key] = mapTypeNameField(key, item, fn)
		}
		if literal, ok := current["map"].(map[string]any); ok {
			if pair, ok := literal["type"].([]any); ok && len(pair) == 2 {
				key, _ := pair[0].(string)
				elem, _ := pair[1].(string)
				fn(wlangtypes.MapType(key, elem))
			}
		}
		return current
	}
	return value
}

func mapTypeNameField(key string, value any, fn func(string) string) any {
	switch key {
	case "zero", "@type":
		if name, ok := value.(string); ok {
			return fn(name)
		}
	case "elem", "returns", "type", "struct", "chan", "params":
		return typeNamePosition(value, fn)
	case "let":
		// 元组解构 [names, types, expr] 的第二项是声明类型。
		if items, ok := value.([]any); ok && len(items) == 3 {
			items[1] = typeNamePosition(items[1], fn)
			items[2] = mapTypeNames(items[2], fn)
			return items
		}
	case "type.assert", "type.assert.ok", "type.is", "ptr.new":
		// 类型名是最后一个实参，写成字符串字面量。
		if args, ok := value.([]any); ok && len(args) > 0 {
			for i := range args[:len(args)-1] {
				args[i] = mapTypeNames(args[i], fn)
			}
			if target, ok := args[len(args)-1].(map[string]any); ok {
				if literal, ok := target["literal"].(map[string]any); ok {
					if name, ok := literal["value"].(string); ok {
						literal["value"] = fn(name)
					}
				}
			}
			return args
		}
	}
	return mapTypeNames(value, fn)
}

// typeNamePosition 改写一个类型名位置：字符串本身，或数组里的字符串（形参的 [name, type] 对里形参名不含包限定，
// 与类型名不会相同）；对象与数组里的表达式照常递归。
func typeNamePosition(value any, fn func(string) string) any {
	switch current := value.(type) {
	case string:
		return fn(current)
	case []any:
		for i, item := range current {
			current[i] = typeNamePosition(item, fn)
		}
		return current
	case map[string]any:
		return mapTypeNames(current, fn)
	}
	return value
}
