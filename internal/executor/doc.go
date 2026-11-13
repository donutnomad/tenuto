// Package executor owns task execution.
//
// It consumes Workflow and Activity tasks, replays executable Workflow JSON,
// invokes Activity handlers, and reports commands or results to the Run layer.
package executor
