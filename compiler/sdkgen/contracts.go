package sdkgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Contract 是源码里一条 `@Activity` 注解解析出的 activity 契约。
//
// 本地实现用实现函数本身作稳定 marker（Symbol = pkg.Charge）；remote 契约用生成器
// 产出的标记函数（Symbol = pkg.ChargeRemoteActivity）。两者的 Type/Queue/Version
// 完全来自注解，不再从函数名猜测。
type Contract struct {
	PkgDir     string // 契约所在包目录（绝对路径）
	PkgName    string // Go 包名
	ImportPath string // 模块内 import path
	Type       string // 注解 type，如 payments.charge
	Queue      string // 注解 queue，空表示用 workflow 的默认 queue
	Version    string // 注解 version
	Remote     bool   // remote=true：契约是命名函数类型，实现不在本仓
	Input      string // 输入类型的源码文本（契约包内的写法，如 ChargeInput）
	Output     string // 输出类型的源码文本
	// InputExpr/OutputExpr 是同一类型的 AST，跨包重写时用于按 import 别名重新限定。
	InputExpr  ast.Expr
	OutputExpr ast.Expr
	// Imports 是契约文件里的 import 别名 → path，跨包限定类型时用。
	Imports map[string]string
	// Structs 是契约包里声明的结构体类型名，用于核对输入输出是 JSON object。
	Structs    map[string]bool
	CallName   string // 生成的桥名，如 CallCharge
	MarkerName string // 稳定 marker 名：实现函数名，或 remote 契约的 <Type>Activity 标记函数
	Pos        string // file:line
}

// markerExpr 是 marker 在 Go 源码里的写法：同包裸标识符，跨包带 import 别名。
func (c Contract) markerExpr(qualifier string) string {
	if qualifier == "" {
		return c.MarkerName
	}
	return qualifier + "." + c.MarkerName
}

// moduleInfo 描述当前仓库的模块根与 import path 前缀。
type moduleInfo struct {
	Root string
	Path string
}

// findModule 从 dir 向上找到 go.mod。
func findModule(dir string) (moduleInfo, error) {
	current, err := filepath.Abs(dir)
	if err != nil {
		return moduleInfo{}, err
	}
	for {
		data, err := os.ReadFile(filepath.Join(current, "go.mod"))
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if rest, ok := strings.CutPrefix(line, "module "); ok {
					return moduleInfo{Root: current, Path: strings.TrimSpace(rest)}, nil
				}
			}
			return moduleInfo{}, fmt.Errorf("%s/go.mod 缺少 module 声明", current)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return moduleInfo{}, fmt.Errorf("%s 之上找不到 go.mod", dir)
		}
		current = parent
	}
}

// dirForImport 把模块内 import path 映射到仓库目录。
func (m moduleInfo) dirForImport(importPath string) (string, bool) {
	if importPath == m.Path {
		return m.Root, true
	}
	rest, ok := strings.CutPrefix(importPath, m.Path+"/")
	if !ok {
		return "", false
	}
	return filepath.Join(m.Root, filepath.FromSlash(rest)), true
}

// importForDir 把仓库目录映射到模块内 import path。
func (m moduleInfo) importForDir(dir string) (string, bool) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(m.Root, absolute)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	if rel == "." {
		return m.Path, true
	}
	return m.Path + "/" + filepath.ToSlash(rel), true
}

// IsGeneratedSource 判断文件是否由代码生成器产出。生成的桥文件不参与注解扫描：
// 否则二次运行会把生成物当输入，形成自循环。
func IsGeneratedSource(src []byte) bool {
	for _, line := range strings.Split(string(src), "\n") {
		if strings.HasPrefix(line, "package ") {
			return false
		}
		if strings.HasPrefix(line, "// Code generated ") && strings.Contains(line, "DO NOT EDIT") {
			return true
		}
	}
	return false
}

// scanContracts 扫描目录里所有非生成、非测试 Go 文件的 @Activity 注解。
// 返回值第二项是该目录参与扫描的源文件，供 CLI 的 watch 使用。
func scanContracts(dir string, module moduleInfo) ([]Contract, []string, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	entries, err := os.ReadDir(absolute)
	if err != nil {
		return nil, nil, err
	}
	importPath, _ := module.importForDir(absolute)
	fset := token.NewFileSet()
	structs := map[string]bool{}
	var contracts []Contract
	var sources []string
	types := map[string]string{}
	callNames := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "zz_") {
			continue
		}
		filename := filepath.Join(absolute, name)
		src, err := os.ReadFile(filename)
		if err != nil {
			return nil, nil, err
		}
		if IsGeneratedSource(src) {
			continue
		}
		file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, filename)
		for name := range fileStructs(file) {
			structs[name] = true
		}
		found, err := fileContracts(fset, file, src, absolute, file.Name.Name, importPath, structs)
		if err != nil {
			return nil, nil, err
		}
		for _, contract := range found {
			if previous, exists := types[contract.Type]; exists {
				return nil, nil, fmt.Errorf("%s: @Activity type=%q 重复声明（另一处在 %s）", contract.Pos, contract.Type, previous)
			}
			types[contract.Type] = contract.Pos
			if previous, exists := callNames[contract.CallName]; exists {
				return nil, nil, fmt.Errorf("%s: 生成的桥名 %s 与 %s 冲突", contract.Pos, contract.CallName, previous)
			}
			callNames[contract.CallName] = contract.Pos
			contracts = append(contracts, contract)
		}
	}
	for _, contract := range contracts {
		if err := requireObjectType(contract, "输入", contract.Input, contract.InputExpr); err != nil {
			return nil, nil, err
		}
		if err := requireObjectType(contract, "输出", contract.Output, contract.OutputExpr); err != nil {
			return nil, nil, err
		}
	}
	sort.Slice(contracts, func(i, j int) bool { return contracts[i].CallName < contracts[j].CallName })
	return contracts, sources, nil
}

// isStringAnyMap 判断类型表达式是否为 map[string]any。
func isStringAnyMap(expr ast.Expr) bool {
	typed, ok := expr.(*ast.MapType)
	if !ok {
		return false
	}
	key, keyOK := typed.Key.(*ast.Ident)
	value, valueOK := typed.Value.(*ast.Ident)
	return keyOK && key.Name == "string" && valueOK && value.Name == "any"
}

// requireObjectType 要求 Activity 的输入或输出是本包的 struct、其他包的命名类型或 map[string]any。
// Activity 的输入输出在线上是 JSON object；其他包的命名类型由 activity.RegisterActivity 在注册时核对。
func requireObjectType(contract Contract, side, source string, expr ast.Expr) error {
	switch typed := expr.(type) {
	case *ast.Ident:
		if contract.Structs[typed.Name] {
			return nil
		}
	case *ast.SelectorExpr:
		return nil
	}
	if isStringAnyMap(expr) {
		return nil
	}
	return fmt.Errorf("%s: @Activity type=%q 的%s类型 %s 不是 JSON object；Activity 的输入输出必须是 struct 或 map[string]any，单个值请包进 struct 的字段",
		contract.Pos, contract.Type, side, source)
}

// fileContracts 提取单个文件里的 activity 契约。structs 是该包已收集的结构体定义。
func fileContracts(fset *token.FileSet, file *ast.File, src []byte, pkgDir, pkgName, importPath string, structs map[string]bool) ([]Contract, error) {
	var contracts []Contract
	for _, decl := range file.Decls {
		switch declaration := decl.(type) {
		case *ast.FuncDecl:
			entries, err := declarationAnnotations(fset, declaration.Doc)
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				continue
			}
			if len(entries) > 1 {
				return nil, fmt.Errorf("%s: 一个声明只能带一条 @Activity 注解", entries[1].pos())
			}
			entry := entries[0]
			if entry.Kind != "Activity" {
				continue
			}
			annotation, err := toActivityAnnotation(entry)
			if err != nil {
				return nil, err
			}
			if annotation.Remote {
				return nil, fmt.Errorf("%s: remote=true 只能标注命名函数类型（type X func(...)），不能标注有函数体的实现", entry.pos())
			}
			if declaration.Recv != nil {
				return nil, fmt.Errorf("%s: @Activity 实现必须是普通函数，不能是方法", entry.pos())
			}
			params := flattenFieldTypes(declaration.Type.Params)
			results := flattenFieldTypes(declaration.Type.Results)
			if err := validateActivitySignature(annotation.Pos, params, results); err != nil {
				return nil, err
			}
			contract := Contract{
				PkgDir:     pkgDir,
				PkgName:    pkgName,
				ImportPath: importPath,
				Type:       annotation.Type,
				Queue:      annotation.Queue,
				Version:    annotation.Version,
				Input:      sourceOf(fset, src, params[1]),
				Output:     sourceOf(fset, src, results[0]),
				InputExpr:  params[1],
				OutputExpr: results[0],
				Imports:    fileImportAliases(file),
				Structs:    structs,
				MarkerName: declaration.Name.Name,
				Pos:        annotation.Pos,
			}
			contract.CallName, err = bridgeCallName(annotation.Type, annotation.Pos)
			if err != nil {
				return nil, err
			}
			contracts = append(contracts, contract)
		case *ast.GenDecl:
			if declaration.Tok != token.TYPE {
				continue
			}
			for _, spec := range declaration.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				entries, err := declarationAnnotations(fset, declaration.Doc, typeSpec.Doc)
				if err != nil {
					return nil, err
				}
				if len(entries) == 0 {
					continue
				}
				if len(entries) > 1 {
					return nil, fmt.Errorf("%s: 一个声明只能带一条 @Activity 注解", entries[1].pos())
				}
				entry := entries[0]
				if entry.Kind != "Activity" {
					continue
				}
				annotation, err := toActivityAnnotation(entry)
				if err != nil {
					return nil, err
				}
				if !annotation.Remote {
					return nil, fmt.Errorf("%s: 命名函数类型 %s 必须声明 remote=true", entry.pos(), typeSpec.Name.Name)
				}
				funcType, ok := typeSpec.Type.(*ast.FuncType)
				if !ok {
					return nil, fmt.Errorf("%s: remote 契约 %s 必须是命名函数类型（type X func(...)）", entry.pos(), typeSpec.Name.Name)
				}
				params := flattenFieldTypes(funcType.Params)
				results := flattenFieldTypes(funcType.Results)
				if err := validateActivitySignature(annotation.Pos, params, results); err != nil {
					return nil, err
				}
				contract := Contract{
					PkgDir:     pkgDir,
					PkgName:    pkgName,
					ImportPath: importPath,
					Type:       annotation.Type,
					Queue:      annotation.Queue,
					Version:    annotation.Version,
					Remote:     true,
					Input:      sourceOf(fset, src, params[1]),
					Output:     sourceOf(fset, src, results[0]),
					InputExpr:  params[1],
					OutputExpr: results[0],
					Imports:    fileImportAliases(file),
					Structs:    structs,
					MarkerName: typeSpec.Name.Name + "Activity",
					Pos:        annotation.Pos,
				}
				contract.CallName, err = bridgeCallName(annotation.Type, annotation.Pos)
				if err != nil {
					return nil, err
				}
				contracts = append(contracts, contract)
			}
		}
	}
	return contracts, nil
}

// fileStructs 收集文件里声明的结构体类型名。
func fileStructs(file *ast.File) map[string]bool {
	structs := map[string]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			if typeSpec, ok := spec.(*ast.TypeSpec); ok {
				if _, isStruct := typeSpec.Type.(*ast.StructType); isStruct {
					structs[typeSpec.Name.Name] = true
				}
			}
		}
	}
	return structs
}

// fileImportAliases 收集文件里的 import 别名 → path；显式别名优先，否则用路径末段。
func fileImportAliases(file *ast.File) map[string]string {
	aliases := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		alias := path
		if index := strings.LastIndex(alias, "/"); index >= 0 {
			alias = alias[index+1:]
		}
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		aliases[alias] = path
	}
	return aliases
}

// validateActivitySignature 强制 `func(ctx, In) (Out, error)` 形状。
func validateActivitySignature(pos string, params, results []ast.Expr) error {
	if len(params) != 2 {
		return fmt.Errorf("%s: activity 契约必须有两个参数 (ctx, in)，实际 %d 个", pos, len(params))
	}
	if !isContextPackageType(params[0]) {
		return fmt.Errorf("%s: activity 契约第一个参数必须是 context.Context，实际 %s", pos, exprString(params[0]))
	}
	if len(results) != 2 {
		return fmt.Errorf("%s: activity 契约必须返回 (out, error)，实际 %d 个返回值", pos, len(results))
	}
	if !isErrorType(results[1]) {
		return fmt.Errorf("%s: activity 契约第二个返回值必须是 error，实际 %s", pos, exprString(results[1]))
	}
	if isErrorType(results[0]) {
		return fmt.Errorf("%s: activity 契约第一个返回值不能是 error", pos)
	}
	return nil
}

// isContextPackageType 判断是不是标准库 context.Context。
func isContextPackageType(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Context" {
		return false
	}
	root, ok := selector.X.(*ast.Ident)
	return ok && root.Name == "context"
}

// sourceOf 取源码里某个表达式的原文。
func sourceOf(fset *token.FileSet, src []byte, expr ast.Expr) string {
	file := fset.File(expr.Pos())
	if file == nil {
		return exprString(expr)
	}
	start, end := file.Offset(expr.Pos()), file.Offset(expr.End())
	if start < 0 || end > len(src) || start >= end {
		return exprString(expr)
	}
	return string(src[start:end])
}

// bridgeCallName 从 activity type 派生桥名：payments.charge → CallCharge，
// payments.charge.v1 → 仍为 CallCharge（末尾的版本段不参与命名）。
func bridgeCallName(activityType, pos string) (string, error) {
	segment := activityBaseSegment(activityType)
	base, err := exportedName(segment)
	if err != nil {
		return "", fmt.Errorf("%s: @Activity type=%q 无法派生 Go 标识符: %w", pos, activityType, err)
	}
	return "Call" + base, nil
}

// activityBaseSegment 取 activity type 的语义段：去掉末尾的版本段（v1 / v2 / 2 / v1beta1）。
//
// 输出只用于派生桥名，不改变 ActivityType 本身。
func activityBaseSegment(activityType string) string {
	segments := strings.Split(activityType, ".")
	for len(segments) > 1 && isVersionSegment(segments[len(segments)-1]) {
		segments = segments[:len(segments)-1]
	}
	return segments[len(segments)-1]
}

// isVersionSegment 判断一段是不是版本标签。
func isVersionSegment(segment string) bool {
	if segment == "" {
		return false
	}
	digits := segment
	if segment[0] == 'v' || segment[0] == 'V' {
		digits = segment[1:]
		if digits == "" {
			return false
		}
	}
	hasDigit := false
	for _, char := range digits {
		switch {
		case char >= '0' && char <= '9':
			hasDigit = true
		case (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z'):
			// v1beta1 这类后缀仍算版本段
		default:
			return false
		}
	}
	if !hasDigit {
		return false
	}
	// 纯字母（如 "charge"）不是版本段。
	if segment[0] != 'v' && segment[0] != 'V' {
		for _, char := range segment {
			if char < '0' || char > '9' {
				return false
			}
		}
	}
	return true
}

// exportedName 把点/下划线/短横线分隔的片段转成导出式驼峰标识符。
func exportedName(raw string) (string, error) {
	var runes []rune
	upperNext := true
	for _, char := range raw {
		switch {
		case unicode.IsLetter(char) || unicode.IsDigit(char):
			if upperNext {
				runes = append(runes, unicode.ToUpper(char))
				upperNext = false
			} else {
				runes = append(runes, char)
			}
		case char == '.' || char == '_' || char == '-' || char == '/':
			upperNext = true
		default:
			return "", fmt.Errorf("非法字符 %q", char)
		}
	}
	if len(runes) == 0 {
		return "", fmt.Errorf("标识符为空")
	}
	if !unicode.IsLetter(runes[0]) {
		return "", fmt.Errorf("标识符 %q 必须以字母开头", string(runes))
	}
	return string(runes), nil
}

// renderBridge 渲染 `zz_<pkg>_contract_gen.go`：每个契约一个 typed 桥 + 稳定 marker。
func renderBridge(pkgName string, contracts []Contract) []byte {
	var builder strings.Builder
	builder.WriteString("// Code generated by tenuto-workflow-gen. DO NOT EDIT.\n\n")
	builder.WriteString("package " + pkgName + "\n\n")
	builder.WriteString("import (\n")
	hasRemote := false
	for _, contract := range contracts {
		if contract.Remote {
			hasRemote = true
			break
		}
	}
	if hasRemote {
		builder.WriteString("\t\"context\"\n\n")
	}
	builder.WriteString("\t\"github.com/donutnomad/tenuto/sdk/activity\"\n")
	builder.WriteString("\tworkflow \"github.com/donutnomad/tenuto/sdk/workflow\"\n")
	builder.WriteString(")\n")
	for _, contract := range contracts {
		q := contract.Queue
		if q == "" {
			q = "workflow 默认队列"
		}
		builder.WriteString("\n// " + contract.CallName + " 调度 activity " + contract.Type +
			"（queue " + q + ", version " + defaultVersionLabel(contract.Version) + "）。\n")
		if contract.Remote {
			builder.WriteString("// 稳定 marker：" + contract.MarkerName + " 是 remote 契约的可导出标记函数。\n")
			builder.WriteString("// runtime 只按该符号 BindSymbol 到 " + contract.Type + "，从不调用它；\n")
			builder.WriteString("// 必须是函数而非变量：go2wlang 只把包级函数翻译成 symbol。\n")
			builder.WriteString("func " + contract.MarkerName + "(_ context.Context, _ " + contract.Input + ") (" + contract.Output + ", error) {\n")
			builder.WriteString("\tpanic(\"activity marker " + contract.MarkerName + " is bound by symbol only\")\n")
			builder.WriteString("}\n\n")
		}
		builder.WriteString("func " + contract.CallName + "(ctx workflow.Context, in " + contract.Input +
			", opts ...workflow.ActivityOptions) (" + contract.Output + ", error) {\n")
		builder.WriteString("\tvar out " + contract.Output + "\n")
		builder.WriteString("\terr := workflow.ExecuteActivity(ctx, " + contract.MarkerName + ", in, opts...).Get(ctx, &out)\n")
		builder.WriteString("\treturn out, err\n")
		builder.WriteString("}\n\n")
		builder.WriteString("var _ activity.Caller[" + contract.Input + ", " + contract.Output + "] = " + contract.CallName + "\n")
		if !contract.Remote {
			builder.WriteString("var _ activity.Handler[" + contract.Input + ", " + contract.Output + "] = " + contract.MarkerName + "\n")
		}
	}
	builder.WriteString(renderRegisterActivities(contracts))
	return []byte(builder.String())
}

// renderRegisterActivities 渲染包级批量注册 helper：`pkg.RegisterActivities(registry)`
// 一行登记本包全部本地实现及其契约。
//
// 没有任何本地实现（remote-only 包）时不渲染：remote 契约的实现由拥有该 activity 的
// worker 注册，生成本包 helper 会让人误以为此处能注册。
func renderRegisterActivities(contracts []Contract) string {
	local := make([]Contract, 0, len(contracts))
	for _, contract := range contracts {
		if !contract.Remote {
			local = append(local, contract)
		}
	}
	if len(local) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("\n// RegisterActivities 把本包全部本地 @Activity 实现登记到 registry。\n")
	builder.WriteString("// 每个实现的契约（type、version、输入输出指纹）由 activity.RegisterActivity 算出，Worker 登记时随 registry 上报。\n")
	builder.WriteString("// remote 契约的实现在别的进程，由那个进程登记。\n")
	builder.WriteString("func RegisterActivities(registry *activity.Registry) error {\n")
	for _, contract := range local {
		builder.WriteString("\tif err := activity.RegisterActivity(registry, " + strconv.Quote(contract.Type) + ", " +
			strconv.Quote(contract.Version) + ", " + contract.MarkerName + "); err != nil {\n\t\treturn err\n\t}\n")
	}
	builder.WriteString("\treturn nil\n}\n")
	return builder.String()
}

func defaultVersionLabel(version string) string {
	if version == "" {
		return "未声明"
	}
	return version
}
