package sdkgen

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

// 注解语法固定为注释行内的 `@Kind(key=value,key="value")`：逗号分隔、key=value，
// 值可带双引号也可不带。这里不用空格分隔，避免与 Go 注释里的自然语言混淆。

// workflowAnnotation 是 `@Workflow(...)` 的解析结果。
type workflowAnnotation struct {
	ID      string
	Name    string
	Queue   string
	Policy  string // 归一化后的策略，缺省 auto_upgrade
	Version string
	Pos     string // file:line
}

// activityAnnotation 是 `@Activity(...)` 的解析结果。
type activityAnnotation struct {
	Type    string
	Queue   string
	Version string
	Remote  bool
	Pos     string // file:line
}

// annotationEntry 是一条已解析的注解及其位置。
type annotationEntry struct {
	Kind   string // "Workflow" | "Activity"
	File   string
	Line   int
	Values map[string]string
}

func (a annotationEntry) pos() string { return fmt.Sprintf("%s:%d", a.File, a.Line) }

var (
	workflowAnnotationKeys = map[string]bool{"id": true, "name": true, "queue": true, "policy": true, "version": true}
	activityAnnotationKeys = map[string]bool{"type": true, "queue": true, "version": true, "remote": true}
)

// parseAnnotationLine 解析一条注解行。text 是去掉注释前缀并 trim 过的内容。
func parseAnnotationLine(fset *token.FileSet, comment *ast.Comment) (annotationEntry, bool, error) {
	position := fset.Position(comment.Pos())
	entry := annotationEntry{File: position.Filename, Line: position.Line}
	raw := comment.Text
	var body string
	switch {
	case strings.HasPrefix(raw, "//"):
		body = strings.TrimSpace(strings.TrimPrefix(raw, "//"))
	case strings.HasPrefix(raw, "/*"):
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "/*"), "*/"))
		if strings.HasPrefix(inner, "@Workflow(") || strings.HasPrefix(inner, "@Activity(") {
			return entry, false, fmt.Errorf("%s: 注解必须写成 // 行注释", entry.pos())
		}
		return entry, false, nil
	default:
		return entry, false, nil
	}
	switch {
	case strings.HasPrefix(body, "@Workflow("):
		entry.Kind = "Workflow"
		body = strings.TrimPrefix(body, "@Workflow(")
	case strings.HasPrefix(body, "@Activity("):
		entry.Kind = "Activity"
		body = strings.TrimPrefix(body, "@Activity(")
	default:
		return entry, false, nil
	}
	if !strings.HasSuffix(body, ")") {
		return entry, false, fmt.Errorf("%s: @%s 注解缺少右括号", entry.pos(), entry.Kind)
	}
	body = strings.TrimSuffix(body, ")")
	values, err := parseAnnotationValues(body)
	if err != nil {
		return entry, false, fmt.Errorf("%s: @%s 注解语法错误: %w", entry.pos(), entry.Kind, err)
	}
	entry.Values = values
	return entry, true, nil
}

// parseAnnotationValues 按逗号切分 key=value 对，value 支持双引号包裹。
func parseAnnotationValues(body string) (map[string]string, error) {
	values := map[string]string{}
	for _, part := range splitAnnotationParts(body) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, rawValue, found := strings.Cut(part, "=")
		key = strings.TrimSpace(key)
		if !found {
			return nil, fmt.Errorf("%q 不是 key=value 形式", part)
		}
		if key == "" {
			return nil, fmt.Errorf("%q 缺少 key", part)
		}
		value, err := annotationValue(rawValue)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("key %q 重复", key)
		}
		values[key] = value
	}
	return values, nil
}

// splitAnnotationParts 按逗号切分，逗号在双引号内时不切。
func splitAnnotationParts(body string) []string {
	var parts []string
	var current strings.Builder
	quoted := false
	escaped := false
	for _, char := range body {
		switch {
		case escaped:
			current.WriteRune(char)
			escaped = false
		case quoted && char == '\\':
			current.WriteRune(char)
			escaped = true
		case char == '"':
			quoted = !quoted
			current.WriteRune(char)
		case char == ',' && !quoted:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(char)
		}
	}
	parts = append(parts, current.String())
	return parts
}

// annotationValue 解析单个值：带引号按 Go 字符串转义，不带引号取原文。
func annotationValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("值为空")
	}
	if strings.HasPrefix(raw, `"`) {
		if !strings.HasSuffix(raw, `"`) || len(raw) < 2 {
			return "", fmt.Errorf("双引号未闭合")
		}
		value, err := strconv.Unquote(raw)
		if err != nil {
			return "", fmt.Errorf("字符串值无法解析: %w", err)
		}
		return value, nil
	}
	if strings.ContainsAny(raw, `"`) {
		return "", fmt.Errorf("值 %q 的引号成对性错误", raw)
	}
	return raw, nil
}

// declarationAnnotations 收集一个声明文档注释里的 @Workflow / @Activity 注解。
func declarationAnnotations(fset *token.FileSet, docs ...*ast.CommentGroup) ([]annotationEntry, error) {
	var out []annotationEntry
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		for _, comment := range doc.List {
			entry, ok, err := parseAnnotationLine(fset, comment)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, entry)
			}
		}
	}
	return out, nil
}

// toWorkflowAnnotation 校验 key 集合并归一化策略。
func toWorkflowAnnotation(entry annotationEntry) (workflowAnnotation, error) {
	if err := checkAnnotationKeys(entry, workflowAnnotationKeys); err != nil {
		return workflowAnnotation{}, err
	}
	annotation := workflowAnnotation{
		ID:      entry.Values["id"],
		Name:    entry.Values["name"],
		Queue:   entry.Values["queue"],
		Policy:  entry.Values["policy"],
		Version: entry.Values["version"],
		Pos:     entry.pos(),
	}
	if annotation.ID == "" {
		return workflowAnnotation{}, fmt.Errorf("%s: @Workflow 缺少 id", entry.pos())
	}
	if annotation.Name == "" {
		annotation.Name = annotation.ID
	}
	switch workflow.Policy(annotation.Policy) {
	case "":
		annotation.Policy = string(workflow.DefaultPolicy)
	case workflow.PolicyAutoUpgrade, workflow.PolicyPinned:
	default:
		return workflowAnnotation{}, fmt.Errorf("%s: @Workflow policy=%q 未知，支持 auto_upgrade、pinned", entry.pos(), annotation.Policy)
	}
	return annotation, nil
}

// toActivityAnnotation 校验 key 集合并解析 remote 布尔值。
func toActivityAnnotation(entry annotationEntry) (activityAnnotation, error) {
	if err := checkAnnotationKeys(entry, activityAnnotationKeys); err != nil {
		return activityAnnotation{}, err
	}
	annotation := activityAnnotation{
		Type:    entry.Values["type"],
		Queue:   entry.Values["queue"],
		Version: entry.Values["version"],
		Pos:     entry.pos(),
	}
	if annotation.Type == "" {
		return activityAnnotation{}, fmt.Errorf("%s: @Activity 缺少 type", entry.pos())
	}
	if raw, exists := entry.Values["remote"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return activityAnnotation{}, fmt.Errorf("%s: @Activity remote=%q 不是布尔值", entry.pos(), raw)
		}
		annotation.Remote = value
	}
	return annotation, nil
}

// checkAnnotationKeys 拒绝未知 key，错误里带上注解位置。
func checkAnnotationKeys(entry annotationEntry, allowed map[string]bool) error {
	keys := make([]string, 0, len(entry.Values))
	for key := range entry.Values {
		keys = append(keys, key)
	}
	for _, key := range sortedStrings(keys) {
		if !allowed[key] {
			return fmt.Errorf("%s: @%s 未知 key %q", entry.pos(), entry.Kind, key)
		}
	}
	return nil
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
