// Package startupflow 演示「构建即携带 Workflow 定义」：作者只写 Go 注解，
// 生成器把 workflow.json embed 进二进制，Worker 启动时自动登记。
package startupflow

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// GreetInput 是 Workflow 入口的结构化输入。
type GreetInput struct {
	Name string `json:"name"`
}

// GreetOutput 是 Workflow 的结构化输出。
type GreetOutput struct {
	Message string `json:"message"`
}

// SayHelloInput 是 Activity 的结构化输入。
type SayHelloInput struct {
	Name string `json:"name"`
}

// SayHelloOutput 是 Activity 的结构化输出。
type SayHelloOutput struct {
	Message string `json:"message"`
}

// @Workflow(id="startup-greeting",name="Startup Greeting",policy=auto_upgrade,version=v1)
func Greet(ctx workflow.Context, in GreetInput) (GreetOutput, error) {
	out, err := CallSayHello(ctx, SayHelloInput{Name: in.Name}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return GreetOutput{}, err
	}
	return GreetOutput{Message: out.Message}, nil
}

// @Activity(type="startup.say-hello",queue="startup",version=v1)
func SayHello(_ context.Context, in SayHelloInput) (SayHelloOutput, error) {
	return SayHelloOutput{Message: "hello " + in.Name}, nil
}
