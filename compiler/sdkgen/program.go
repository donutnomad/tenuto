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

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/wlang/go2wlang"
)

// Plan 是注解生成的第一阶段结果：注解、契约、待写桥文件与 watch 输入。
//
// 两阶段是有原因的：桥文件把 CallXXX 与 remote marker 变成真实 Go 符号，翻译器要
// 用 packages.Load 对包做类型检查。CLI 先把 plan.Bridges 落盘，再调 Generate 重写并
// 翻译——首次生成因此不依赖任何历史产物；读注解时刻意跳过生成文件，避免自循环。
type Plan struct {
	Dir        string
	ImportPath string
	// PackageName 是工作流源码包的包名，用于渲染 zz_<pkg>_artifacts_gen.go。
	PackageName string
	Spec        workflowSpec
	// Contracts 是「import path → 该包契约」，含工作流包与被引用的本地契约包。
	Contracts map[string][]Contract
	// Bridges 是各契约包的 `zz_<pkg>_contract_gen.go`。
	Bridges []GeneratedFile
	// WatchFiles 是本次读取的源码（绝对路径）。
	WatchFiles []string
	// Source 是工作流源码原文。
	Source []byte
}

// PackageResult 是一次 `tenuto-workflow-gen generate <pkgdir>` 的完整产物。
type PackageResult struct {
	// Dir 是工作流源码目录（绝对路径）。
	Dir string
	// PackageName 是工作流源码包的包名。
	PackageName string
	// WorkflowType 来自 @Workflow(type=...)。
	WorkflowType string
	// Policy 与 Version 来自 @Workflow(policy=...,version=...)，同时写进 WorkflowConfig。
	Policy  string
	Version string
	// Workflow 是登记用的配置，Program 内联其中。
	Workflow *programpkg.WorkflowConfig
	// Bridges 是本次生成使用的桥文件（与 Plan.Bridges 相同，便于调用方复查）。
	Bridges []GeneratedFile
	// WatchFiles 是本次生成读过的源码。
	WatchFiles []string
	// Embedded 是随二进制发布的 zz_<pkg>_artifacts_gen.go（embed workflow.json）。
	Embedded GeneratedFile
}

// GeneratedFile 是一个待写出的生成文件。
type GeneratedFile struct {
	Path    string
	Content []byte
}

// Scan 收集目录里的 @Workflow/@Activity 注解并渲染桥文件，不做翻译。
// 纯契约包的 Plan.Spec.File 为空，只生成桥文件。
func Scan(dir string) (Plan, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return Plan{}, err
	}
	module, err := findModule(absolute)
	if err != nil {
		return Plan{}, err
	}
	importPath, ok := module.importForDir(absolute)
	if !ok {
		return Plan{}, fmt.Errorf("%s 不在模块 %s 内", absolute, module.Path)
	}
	ownContracts, ownSources, err := scanContracts(absolute, module)
	if err != nil {
		return Plan{}, err
	}
	spec, specSources, err := scanWorkflowSpec(absolute)
	if err != nil {
		return Plan{}, err
	}
	contracts := map[string][]Contract{importPath: ownContracts}
	watchFiles := append(append([]string{}, specSources...), ownSources...)
	if spec.File == "" {
		if len(ownContracts) == 0 {
			return Plan{}, fmt.Errorf("%s: 既没有 @Workflow 也没有 @Activity 注解", dir)
		}
		return Plan{Dir: absolute, ImportPath: importPath, Contracts: contracts,
			Bridges: bridgeFiles(ownContracts), WatchFiles: sortedUnique(watchFiles)}, nil
	}
	source, err := os.ReadFile(spec.File)
	if err != nil {
		return Plan{}, err
	}
	packageName, err := packageClauseName(source)
	if err != nil {
		return Plan{}, err
	}
	for _, importedPath := range importPathsOf(source) {
		importedDir, inModule := module.dirForImport(importedPath)
		if !inModule {
			continue
		}
		importedContracts, sources, err := scanContracts(importedDir, module)
		if err != nil {
			return Plan{}, err
		}
		if len(importedContracts) == 0 {
			continue
		}
		contracts[importedPath] = importedContracts
		watchFiles = append(watchFiles, sources...)
	}
	return Plan{
		Dir:         absolute,
		ImportPath:  importPath,
		PackageName: packageName,
		Spec:        spec,
		Contracts:   contracts,
		Bridges:     bridgeFiles(allContractsInOrder(contracts)),
		WatchFiles:  sortedUnique(watchFiles),
		Source:      source,
	}, nil
}

// Generate 在 Plan 之上重写编排源码并翻译成 WLang，把注解里的 policy/version/queue
// 写进 WorkflowConfig。调用前 Plan.Bridges 必须已经落盘。
func Generate(plan Plan) (PackageResult, error) {
	impls := map[string]bool{}
	for name := range indexActivityImplementations(plan.Contracts[plan.ImportPath]) {
		impls[name] = true
	}
	sources, err := collectWorkflowSources(plan.Source, plan.Spec.File, plan.Dir, plan.Spec.FuncName, impls)
	if err != nil {
		return PackageResult{}, err
	}
	// 桥调用的重写对 workflow 函数与它调用的同包函数逐个进行；被调用的契约按出现顺序去重。
	var rewritten rewriteResult
	usedAt := map[string]bool{}
	for _, fn := range sources.funcs {
		result, err := rewriteWorkflowCalls(sources.files[fn.file], fn.file, fn.key, plan.ImportPath, plan.Contracts)
		if err != nil {
			return PackageResult{}, err
		}
		sources.files[fn.file] = result.Source
		for _, contract := range result.Used {
			if !usedAt[contract.Pos] {
				usedAt[contract.Pos] = true
				rewritten.Used = append(rewritten.Used, contract)
			}
		}
	}
	probeTypes, ids, err := probeContractDescriptors(rewritten.Used)
	if err != nil {
		return PackageResult{}, err
	}
	generated, err := generateWithExtraTypes(sources, Options{
		WorkflowType: plan.Spec.Annotation.Type,
		Name:         plan.Spec.Annotation.Name,
		FuncName:     plan.Spec.FuncName,
		Filename:     plan.Spec.File,
		Dir:          plan.Dir,
	}, probeTypes)
	if err != nil {
		return PackageResult{}, err
	}
	fingerprints := make(map[string]struct{ input, output string }, len(rewritten.Used))
	for _, contract := range rewritten.Used {
		input, output, err := ids.wireFingerprints(contract, generated.Workflow.Program.Types)
		if err != nil {
			return PackageResult{}, err
		}
		fingerprints[contract.Pos] = struct{ input, output string }{input, output}
	}
	result := PackageResult{
		Dir:          plan.Dir,
		PackageName:  plan.PackageName,
		WorkflowType: plan.Spec.Annotation.Type,
		Policy:       plan.Spec.Annotation.Policy,
		Version:      plan.Spec.Annotation.Version,
		Workflow:     generated.Workflow,
		WatchFiles:   plan.WatchFiles,
		Bridges:      plan.Bridges,
	}
	// Activities 以翻译产物里真实出现的符号为准：注解契约按注解写 Type/Queue，
	// 未注解的直调（ExecuteActivity + pkg.Symbol）保留原有的 Type=Symbol 约定。
	programSymbols := collectProgramSymbols(generated.Workflow.Program.Program)
	existing := generated.Workflow.Activities
	refs := make(map[string]programpkg.ActivityRef, len(programSymbols.symbols))
	for symbol := range programSymbols.symbols {
		ref := existing[symbol]
		ref.Symbol = symbol
		refs[symbol] = ref
	}
	for _, contract := range rewritten.Used {
		symbol, err := matchProgramSymbol(programSymbols, contract)
		if err != nil {
			return PackageResult{}, err
		}
		fingerprint := fingerprints[contract.Pos]
		refs[symbol] = programpkg.ActivityRef{
			Symbol:    symbol,
			Type:      contract.Type,
			TaskQueue: contract.Queue,
			Input:     fingerprint.input,
			Output:    fingerprint.output,
		}
	}
	result.Workflow.Activities = refs
	result.Workflow.Policy = plan.Spec.Annotation.Policy
	result.Workflow.Version = plan.Spec.Annotation.Version
	result.Embedded = embeddedArtifactsFile(plan.Dir, plan.PackageName)
	return result, nil
}

// programMarkers 是 WLang 程序里 ExecuteActivity 的 marker 写法：真正的函数符号，
// 以及被翻译成变量引用的标识符（局部变量/函数值遮蔽，或包未完成类型检查）。
type programMarkers struct {
	symbols map[string]bool
	vars    map[string]bool
}

// collectProgramSymbols 收集 WLang 程序里 ExecuteActivity 的 marker 写法。
func collectProgramSymbols(program []any) programMarkers {
	markers := programMarkers{symbols: map[string]bool{}, vars: map[string]bool{}}
	collectSymbols(program, markers)
	return markers
}

// collectSymbols 递归扫描 WLang 程序，记录 ExecuteActivity 的第 2 个业务参数（marker）。
func collectSymbols(node any, out programMarkers) {
	switch value := node.(type) {
	case map[string]any:
		if raw, ok := value["ExecuteActivity"]; ok {
			if args, ok := raw.([]any); ok && len(args) >= 3 {
				if marker, ok := args[2].(map[string]any); ok {
					if symbol, ok := marker["symbol"].(string); ok {
						out.symbols[symbol] = true
					}
					if name, ok := marker["var"].(string); ok {
						out.vars[name] = true
					}
				}
			}
		}
		for _, item := range value {
			collectSymbols(item, out)
		}
	case []any:
		for _, item := range value {
			collectSymbols(item, out)
		}
	}
}

// matchProgramSymbol 找出契约 marker 在产物里的真实符号。
//
// 同包 marker 在产物里带本包前缀（orders.Charge），跨包带 import 别名
// （inventory.Reserve）；以产物为准，避免在生成器里复制翻译器的命名规则。
func matchProgramSymbol(markers programMarkers, contract Contract) (string, error) {
	var matched []string
	for symbol := range markers.symbols {
		if markerMatches(symbol, contract.MarkerName) {
			matched = append(matched, symbol)
		}
	}
	switch len(matched) {
	case 1:
		return matched[0], nil
	case 0:
		return "", markerMissingError(markers, contract)
	default:
		return "", fmt.Errorf("%s: activity %s 的 marker %s 匹配到多个符号 %v，请检查标记名冲突",
			contract.Pos, contract.Type, contract.MarkerName, sortedStrings(matched))
	}
}

// markerMatches 判断产物符号是否就是该 marker（完全同名，或以 .MarkerName 结尾）。
func markerMatches(symbol, marker string) bool {
	return symbol == marker || strings.HasSuffix(symbol, "."+marker)
}

// markerMissingError 区分「marker 根本没出现」与「marker 被当成变量引用」。
//
// 后者是 Go 语义正确的降级：同包 activity 实现名被函数体内的局部变量、参数或函数值
// 遮蔽时，重写出来的 marker 标识符指向那个局部声明，翻译器就该输出变量引用而不是符号。
// 这时必须把原因写清楚，否则只剩「找不到 marker（实际符号 []）」，与真正的原因（遮蔽）
// 相距太远。
func markerMissingError(markers programMarkers, contract Contract) error {
	var shadowed []string
	for name := range markers.vars {
		if markerMatches(name, contract.MarkerName) {
			shadowed = append(shadowed, name)
		}
	}
	if len(shadowed) > 0 {
		return fmt.Errorf("%s: activity %s 的 marker %s 在翻译产物里是变量引用 %v 而不是函数符号；两种原因：所在包没有完成类型检查（含包加载失败），或“%s”在函数体内被同名局部变量/参数/函数值遮蔽。先确认该包能通过类型检查，再确认函数体内没有同名局部声明",
			contract.Pos, contract.Type, contract.MarkerName, sortedStrings(shadowed), contract.MarkerName)
	}
	return fmt.Errorf("%s: 翻译产物里找不到 activity %s 的 marker %s（实际符号 %v）",
		contract.Pos, contract.Type, contract.MarkerName, sortedStrings(keysOf(markers.symbols)))
}

func keysOf(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// descriptorIDs 是每个契约在 Program.Types 里的 Input/Output 描述符 ID，按注解位置索引。
type descriptorIDs map[string]struct{ input, output string }

// wireFingerprints 把描述符 ID 换成与 Go 包名/类型名无关的 wire 指纹。
//
// 服务端声明侧用 config.WLangWireFingerprint、worker 侧用
// activity.ContractOf / WireFingerprintOf 计算同一算法；两侧都写指纹才谈得上
// contract 对照。descriptor 缺失时报 file:line。
func (d descriptorIDs) wireFingerprints(contract Contract, types map[string]programpkg.WLangTypeDescriptor) (string, string, error) {
	ids := d[contract.Pos]
	fingerprint := func(field, id string) (string, error) {
		if id == "" {
			return "", nil
		}
		value, err := programpkg.WLangWireFingerprint(id, types)
		if err != nil {
			return "", fmt.Errorf("%s: activity %s 的 %s 描述符 %q 无法转成 wire 指纹: %w",
				contract.Pos, contract.Type, field, id, err)
		}
		return value, nil
	}
	input, err := fingerprint("input", ids.input)
	if err != nil {
		return "", "", err
	}
	output, err := fingerprint("output", ids.output)
	if err != nil {
		return "", "", err
	}
	return input, output, nil
}

// workflowSpec 是一个目录里唯一的 @Workflow 声明。
type workflowSpec struct {
	Annotation workflowAnnotation
	FuncName   string
	File       string
}

// scanWorkflowSpec 在目录里找唯一的 @Workflow 注解及其函数。
func scanWorkflowSpec(dir string) (workflowSpec, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return workflowSpec{}, nil, err
	}
	fset := token.NewFileSet()
	var specs []workflowSpec
	var sources []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "zz_") {
			continue
		}
		filename := filepath.Join(dir, name)
		src, err := os.ReadFile(filename)
		if err != nil {
			return workflowSpec{}, nil, err
		}
		if IsGeneratedSource(src) {
			continue
		}
		file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
		if err != nil {
			return workflowSpec{}, nil, err
		}
		sources = append(sources, filename)
		for _, declaration := range file.Decls {
			annotations, err := declarationAnnotations(fset, declarationDocs(declaration)...)
			if err != nil {
				return workflowSpec{}, nil, err
			}
			for _, entry := range annotations {
				if entry.Kind != "Workflow" {
					continue
				}
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok {
					return workflowSpec{}, nil, fmt.Errorf("%s: @Workflow 注解只能写在函数上", entry.pos())
				}
				if fn.Recv != nil {
					return workflowSpec{}, nil, fmt.Errorf("%s: @Workflow 函数不能是方法", entry.pos())
				}
				annotation, err := toWorkflowAnnotation(entry)
				if err != nil {
					return workflowSpec{}, nil, err
				}
				specs = append(specs, workflowSpec{Annotation: annotation, FuncName: fn.Name.Name, File: filename})
			}
		}
	}
	switch len(specs) {
	case 0:
		return workflowSpec{}, sources, nil
	case 1:
		return specs[0], sources, nil
	default:
		return workflowSpec{}, nil, fmt.Errorf("%s: @Workflow 注解只能有一个，实际 %d 个（%s 与 %s）",
			dir, len(specs), specs[0].Annotation.Pos, specs[1].Annotation.Pos)
	}
}

// declarationDocs 取声明上可能挂注解的注释组。
func declarationDocs(declaration ast.Decl) []*ast.CommentGroup {
	switch d := declaration.(type) {
	case *ast.FuncDecl:
		return []*ast.CommentGroup{d.Doc}
	case *ast.GenDecl:
		groups := []*ast.CommentGroup{d.Doc}
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				groups = append(groups, s.Doc)
			case *ast.ValueSpec:
				groups = append(groups, s.Doc)
			}
		}
		return groups
	}
	return nil
}

// importPathsOf 取源码文件 import 的路径。
func importPathsOf(src []byte) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", src, parser.ImportsOnly)
	if err != nil {
		return nil
	}
	var paths []string
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		paths = append(paths, path)
	}
	return paths
}

// probeContractDescriptors 用一次签名翻译取出每个契约 Input/Output 的描述符 ID，
// 并返回包含嵌套字段的完整描述符集合。
func probeContractDescriptors(contracts []Contract) (map[string]programpkg.WLangTypeDescriptor, descriptorIDs, error) {
	ids := descriptorIDs{}
	types := map[string]programpkg.WLangTypeDescriptor{}
	for _, contract := range contracts {
		probe := "package " + contract.PkgName + "\n\n" +
			"func zzActivityContractProbe(_ any, in " + contract.Input + ") (" + contract.Output + ", error) {\n" +
			"\tvar zero " + contract.Output + "\n" +
			"\treturn zero, nil\n" +
			"}\n"
		translated, err := go2wlang.TranslateFileDetailed([]byte(probe), go2wlang.Options{
			FuncName: "zzActivityContractProbe",
			Filename: "zz_activity_contract_probe_gen.go",
			Dir:      contract.PkgDir,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("%s: 解析 activity 契约类型失败: %w", contract.Pos, err)
		}
		if translated.Entry.Input == "" {
			return nil, nil, fmt.Errorf("%s: activity %s 的输入类型 %q 没有描述符（不支持的类型）", contract.Pos, contract.Type, contract.Input)
		}
		if translated.Entry.Output == "" {
			return nil, nil, fmt.Errorf("%s: activity %s 的输出类型 %q 没有描述符（不支持的类型）", contract.Pos, contract.Type, contract.Output)
		}
		ids[contract.Pos] = struct{ input, output string }{translated.Entry.Input, translated.Entry.Output}
		for id, descriptor := range translated.Types {
			types[id] = descriptor
		}
	}
	return types, ids, nil
}

// allContractsInOrder 把包 → 契约映射展平成稳定顺序。
func allContractsInOrder(byImportPath map[string][]Contract) []Contract {
	paths := make([]string, 0, len(byImportPath))
	for path := range byImportPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var out []Contract
	for _, path := range paths {
		out = append(out, byImportPath[path]...)
	}
	return out
}

// bridgeFiles 按包渲染 `zz_<pkg>_contract_gen.go`，每个契约包一份。
func bridgeFiles(contracts []Contract) []GeneratedFile {
	grouped := map[string][]Contract{}
	var dirs []string
	for _, contract := range contracts {
		if _, exists := grouped[contract.PkgDir]; !exists {
			dirs = append(dirs, contract.PkgDir)
		}
		grouped[contract.PkgDir] = append(grouped[contract.PkgDir], contract)
	}
	sort.Strings(dirs)
	files := make([]GeneratedFile, 0, len(dirs))
	for _, dir := range dirs {
		pkgContracts := grouped[dir]
		sort.Slice(pkgContracts, func(i, j int) bool { return pkgContracts[i].CallName < pkgContracts[j].CallName })
		files = append(files, GeneratedFile{
			Path:    filepath.Join(dir, "zz_"+pkgContracts[0].PkgName+"_contract_gen.go"),
			Content: renderBridge(pkgContracts[0].PkgName, pkgContracts),
		})
	}
	return files
}

// sortedUnique 去重并排序路径，保证产物与 watch 输入稳定。
func sortedUnique(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
