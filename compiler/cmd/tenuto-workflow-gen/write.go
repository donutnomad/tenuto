package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// marshalJSON 输出稳定、可审阅的 JSON。
func marshalJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// writeIfChanged 只在内容变化时写文件，并打印写出路径。
//
// 二次运行必须字节稳定：相同内容不重写，避免 watch 自触发和无关 diff。
func writeIfChanged(path string, data []byte, stdout io.Writer) error {
	existing, err := os.ReadFile(path)
	if err == nil && bytes.Equal(existing, data) {
		fmt.Fprintf(stdout, "up-to-date %s\n", path)
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("写入 %s 失败: %w", path, err)
	}
	fmt.Fprintf(stdout, "generated %s\n", path)
	return nil
}
